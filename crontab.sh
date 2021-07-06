#!/bin/bash

echo 'start cron job...';

cd ~/go/src/github.com/ndcinfra/platform-batch-maf/
./platform-batch-maf

echo 'success...';
