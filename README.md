수작업시 실행 방법  
        1. DataInfra 서버 접속  
        2. cd ~/go/src/github.com/ndcinfra/platform-batch-maf  
        3. go run main.go -date=2021-06-11  
        (2021-06-11은 원하는 일자)


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
  
