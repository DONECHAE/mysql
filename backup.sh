#!/bin/bash

# MySQL 데이터베이스 백업
mysqldump -u pdc -p 134 CJD > /home/one/mysql3/mysql/backup_$(date +\%F).sql

# Git 커밋 및 푸시 작업
cd /home/one/mysql3/mysql
git add backup_$(date +\%F).sql
git commit -m "Backup on $(date +\%F at %T)"
git push origin main
