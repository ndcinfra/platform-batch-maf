package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
	"github.com/ndcinfra/platform-batch-maf/libs"
)

type Request struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
	Name      string `json:"name"`
	Pid       string `json:"pid"`
}

type Response struct {
	Date         string    `json:"date"`
	Dau          int64     `json:"dau"`
	NewUserCount int64     `json:"new_user_count"`
	SaleUsdSum   float64   `json:"sale_usd_sum"`
	SaleUsdIos   float64   `json:"sale_usd_ios"`
	SaleUsdAos   float64   `json:"sale_usd_aos"`
	IOSTotal     []float64 `json:"ios_total"`
	AOSTotal     []float64 `json:"aos_total"`
	//TotalRev     []float64 `json:"total_rev,omitempty"`
}

type DBfield struct {
	rev_d           float64 // 토탈 금액.(환율적용)
	rev_t           float64 // 토탈 금액. (환율미적용)
	rev_inapp_ios_d float64 // ios 인앱 결제. 환율 적용
	rev_inapp_ios_t float64 // ios 인앱 결제. 환율 미적용
	rev_inapp_aos_d float64 // aos 인앱 결제. 환율 적용
	rev_inapp_aos_t float64 // aos 인앱 결제. 환율 미적용
	rev_ad_ios_d    float64 // ios 광고. 환율 적용
	rev_ad_ios_t    float64 // ios 광고. 환율 미적용
	rev_ad_aos_d    float64 // aos 광고. 환율 적용
	rev_ad_aos_t    float64 // aos 광고. 환율 미적용
}

var insertSql = " INSERT INTO public.kpi(" +
	" territory, date, uu_d, nru_d, rev_d, rev_t, rev_rate, game," +
	"rev_inapp_ios_d, rev_inapp_ios_t,rev_inapp_aos_d, rev_inapp_aos_t," +
	"rev_ad_ios_d, rev_ad_ios_t, rev_ad_aos_d, rev_ad_aos_t) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) "

var db *sql.DB

// 수작업시 실행 방법
//  go run main.go -date=2021-06-11
func main() {
	start := time.Now()

	date := flag.String("date", "", "")
	flag.Parse()

	fmt.Printf("Start Get maf Data !\n")
	err := godotenv.Load()
	if err != nil {
		logs.Error("Error loading .env file")
	}

	//logging
	logs.SetLogger(logs.AdapterFile, `{"filename":"./logs/project.log","level":7,"maxlines":0,"maxsize":0,"daily":true,"maxdays":7,"color":true}`)

	//
	DB_CON := os.Getenv("DBHOST")
	conn, err := pgx.Connect(context.Background(), DB_CON)
	if err != nil {
		logs.Error("Unable to connect to database: %v", err)
		fmt.Printf("Unable to connect to database: %v\n", err)
		os.Exit(1)
		//fmt.Printf("Unable to connect to database: %v\n", err)
	}

	defer conn.Close(context.Background())

	logs.Info("connection success")

	resultDate := Run(*date, conn)

	fmt.Printf("End Get Game Data !\n")

	end := time.Now()
	elapsed := time.Since(start)
	logs.Info("finish: ", end, " , elapsed: ", elapsed)

	libs.SendEmail(resultDate, start.String(), end.String(), elapsed.String())

	os.Exit(0)

}

/// date: YYYY-MM-DD
func Run(date string, conn *pgx.Conn) string {

	currentTime := time.Now().AddDate(0, 0, -1)

	if date != "" {
		currentTime, _ = time.Parse("2006-01-02", date)
	}

	err := godotenv.Load()
	if err != nil {
		logs.Error("ERROR load env: %v\n", err)
		//log.Fatalf("ERROR load env: %v\n", err)
		os.Exit(1)
	}

	// TODO:
	// 'tilerpg' : '게임이 망했다'. ios: id954182728, android: com.maf.tilerpg
	// 'moneyhero' : '중년기사 김봉식". ios: id987072755, android: com.maf.moneyhero
	// 'cattycoon' : '고양이 타이쿤'. ios: id1561080503, android: com.mafgames.idle.cat.neko.manager.tycoon
	// 'hamster' : '햄스터 타이쿤'. ios: id1526108438, android: com.maf.projectH
	// 'newlife' : 'Prison Tycoon' ios: id1547370424, android: com.maf.newlife
	// Train Village:  'Train Town' iOS: id1471953868, android:com.maf.mergeplanet !!! not yet
	// tycooncookie : 쿠키 타이쿤, IOS : 1565768885, AOS : com.maf.idle.hamster.factory.manager.cookie
	// tycoonpuppycafe: 강아지카페 IOS : 1574143572, AOS : com.maf.idle.puppy.cafe.tycoon
	// monstereater: monstereater IOS : 1574143572, AOS : com.maf.idle.puppy.cafe.tycoon

	var reqData Request

	reqData.StartDate = currentTime.Format("2006-01-02")
	reqData.EndDate = currentTime.Format("2006-01-02")
	gameList := strings.Split(os.Getenv("game"), ",")

	currency, err := calcurateCurrencyV2()
	if err != nil {
		logs.Error("Failed to get currency. %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Today currency: %v\n", currency.Rates.KRW)

	fCurrencyRate, _ := strconv.ParseFloat(currency.Rates.KRW, 64)
	fmt.Printf("Float currency: %v\n", fCurrencyRate)

	log.Printf("game list: %v, date: %v\v", gameList, reqData.StartDate)

	for _, name := range gameList {
		var result Response

		reqData.Name = name
		log.Printf("game : %v\v", reqData.Name)

		//call maf api
		url := os.Getenv("MAF_URL")
		method := http.MethodPost

		bData, err := json.Marshal(reqData)
		if err != nil {
			logs.Error("ERROR marshal: %v\n", err)
			//os.Exit(1)
		}

		statusCode, resp, err := callAPI(url, method, string(bData))
		//defer resp.Body.Close()
		if err != nil {
			logs.Error("ERROR: callapi maf. %v\n", err)
			//os.Exit(1)
			continue
		}

		if statusCode >= 400 {
			logs.Error("ERROR: response code from maf. %d\n", statusCode)
			//os.Exit(1)
			continue
		} else {
			respData, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logs.Error("ERROR: read resp.Body: %v\n", err)
				//os.Exit(1)
				continue
			} else {
				//log.Printf("Success. response data from maf: %v\n", string(respData))

				var decoded []interface{}
				err = json.Unmarshal(respData, &decoded)
				if err != nil {
					logs.Error("ERROR unmarshal maf response data: %v\n", err)
					//os.Exit(1)
					continue
				}
				//log.Printf("Success. decoded data from maf: %v\n", decoded[0])

				bd, err := json.Marshal(decoded[0])
				if err != nil {
					logs.Error("ERROR marshal decoded data: %v\n", err)
					//os.Exit(1)
					continue
				}

				json.Unmarshal(bd, &result)
				//log.Printf("Success. response data from maf: %v\n", result)

				// call appfler with game name and ios, android
				appflyerGameList := strings.Split(os.Getenv(reqData.Name), ",")
				log.Printf("appflyer guid: %v\n", appflyerGameList)

				for j, aguid := range appflyerGameList {
					reqData.Pid = aguid

					appflyerURL := os.Getenv("APPFLYER")
					appflyserToken := os.Getenv("APPTOEKN")

					//URL = "https://hq1.appsflyer.com/aggreports/enc/id954182728/partners_report/v5?api_token=51706054-156b-4bb7-92d5-68e722fceff5&from=2021-05-11&to=2021-05-11"
					url = fmt.Sprintf(appflyerURL+"%s"+"/partners_report/v5?api_token=%s&from=%s&to=%s", reqData.Pid, appflyserToken, reqData.StartDate, reqData.EndDate)
					method = http.MethodGet

					fileName := fmt.Sprintf("./tmp/%s.csv", reqData.Pid)

					file, err := os.Create(fileName)
					if err != nil {
						logs.Error("ERROR file create: %+v", err)
						//os.Exit(1)
						continue
					}
					//defer file.Close()

					statusCode, resp, err = callAPI(url, method, string(bData))
					defer resp.Body.Close()
					if err != nil {
						logs.Error("ERROR: %v\n", err)
						//os.Exit(1)
						continue
					}

					if statusCode >= 400 {
						logs.Error("ERROR: %d\n", statusCode)
						//os.Exit(1)
						continue
					} else {

						size, err := io.Copy(file, resp.Body)
						if err != nil {
							logs.Error("ERROR: make file: %v\n", err)
							//os.Exit(1)
							continue
						} else {
							log.Printf("Downloaded a file %s with size %d", fileName, size)
							// read file

							csvData := ReadCsvFile(fileName)
							//fmt.Println(csvData)
							for i := 1; i < len(csvData); i++ {
								f, _ := strconv.ParseFloat(csvData[i], 64)

								switch j {
								case 0:
									// ios
									result.IOSTotal = append(result.IOSTotal, f)
								case 1:
									//and
									result.AOSTotal = append(result.AOSTotal, f)
								case 2:
									//one store  to and
									result.AOSTotal = append(result.AOSTotal, f)
								}

								//result.TotalRev = append(result.TotalRev, f)
							}

						}

					}

					file.Close()
				}

			}

		}

		var dbfield DBfield

		dbfield.rev_inapp_ios_d = result.SaleUsdIos * fCurrencyRate // krw
		dbfield.rev_inapp_ios_t = result.SaleUsdIos                 //usd
		dbfield.rev_inapp_aos_d = result.SaleUsdAos * fCurrencyRate // krw
		dbfield.rev_inapp_aos_t = result.SaleUsdAos                 //usd

		// ad
		//appflyer. ad. ios. usd
		for _, v := range result.IOSTotal {
			dbfield.rev_ad_ios_d += v * fCurrencyRate // krw
			dbfield.rev_ad_ios_t += v
		}

		// ad.
		// appflyer. aos.
		if reqData.Name == "tilerpg" {
			// ad
			//appflyer. ad. aos. krw
			for _, v := range result.AOSTotal {
				dbfield.rev_ad_aos_d += v
				dbfield.rev_ad_aos_t += v / fCurrencyRate
			}
		} else {
			// ad
			//appflyer. ad. aos. usd
			for _, v := range result.AOSTotal {
				dbfield.rev_ad_aos_d += v * fCurrencyRate // krw
				dbfield.rev_ad_aos_t += v
			}
		}

		dbfield.rev_d = dbfield.rev_inapp_ios_d + dbfield.rev_inapp_aos_d + dbfield.rev_ad_ios_d + dbfield.rev_ad_aos_d
		dbfield.rev_t = dbfield.rev_inapp_ios_t + dbfield.rev_inapp_aos_t + dbfield.rev_ad_ios_t + dbfield.rev_ad_aos_t

		log.Printf("result: %s, %v, %v\n", reqData.Name, result, dbfield)

		// rev_inapp_ios_d, rev_inapp_ios_t,rev_inapp_aos_d, rev_inapp_aos_t,
		// rev_ad_ios_d, rev_ad_ios_t, rev_ad_aos_d, rev_ad_aos_t

		// insert db
		_, err = conn.Exec(
			context.Background(),
			insertSql,
			reqData.Name,
			reqData.StartDate,
			result.Dau,
			result.NewUserCount,
			dbfield.rev_d,
			dbfield.rev_t,
			fCurrencyRate,
			reqData.Name,
			dbfield.rev_inapp_ios_d, dbfield.rev_inapp_ios_t, dbfield.rev_inapp_aos_d, dbfield.rev_inapp_aos_t,
			dbfield.rev_ad_ios_d, dbfield.rev_ad_ios_t, dbfield.rev_ad_aos_d, dbfield.rev_ad_aos_t,
		)
		if err != nil {
			logs.Info("DB Error: %v\n", err)
		}

		result.SaleUsdSum = 0
		result.SaleUsdIos = 0
		result.SaleUsdAos = 0
		result.IOSTotal = []float64{}
		result.AOSTotal = []float64{}

	}

	// delete files
	err = os.Remove("./tmp/*.csv")
	if err != nil {
		fmt.Printf("Error remove files. %v\n", err)
	}

	return date

}

func ReadCsvFile(filePath string) []string {
	// Load a csv file.
	f, _ := os.Open(filePath)
	defer f.Close()

	var result []string

	// Create a new reader.
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		result = append(result, record[11])
	}

	return result
}

func callAPI(url, method, data string) (int, *http.Response, error) {
	var req *http.Request
	var err error

	if method == http.MethodPost {
		req, err = http.NewRequest(method, url, strings.NewReader(data))
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
	} else if method == http.MethodGet {
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
	}

	//log.Println(req)

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	log.Println(resp.StatusCode)

	return http.StatusOK, resp, nil
}

type Currency struct {
	USDKRW []float64 `json:"USDKRW"`
	Rates  Rates     `json:"rates"`
}

type Rates struct {
	KRW string `json:"KRW"`
}

func calcurateCurrency() (Currency, error) {
	url := fmt.Sprintf("https://exchange.jaeheon.kr:23490/query/USDKRW")

	req, _ := http.NewRequest("GET", url, nil)
	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))

	if res.StatusCode > 300 {
		return Currency{}, errors.New("Fail get currency.")
	}

	var currency Currency
	err := json.Unmarshal(body, &currency)
	if err != nil {
		return Currency{}, err
	}
	return currency, nil
}

func calcurateCurrencyV2() (Currency, error) {
	url := fmt.Sprintf("https://api.currencyfreaks.com/latest?apikey=e247b898c2d74be89e14126d72f94640")

	req, _ := http.NewRequest("GET", url, nil)
	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))

	if res.StatusCode > 300 {
		return Currency{}, errors.New("Fail get currency.")
	}

	var currency Currency
	err := json.Unmarshal(body, &currency)
	if err != nil {
		return Currency{}, err
	}
	return currency, nil
}
