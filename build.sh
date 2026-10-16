#!/bin/sh
set -e
cd "$(dirname "$0")"
echo "off server"
systemctl stop xiro
systemctl stop xiro-adm
systemctl stop xiro-bot
echo "git pull..."
git pull

echo "building test-admin..."
go build -o bin/telesrv-admin ./cmd/telesrv-admin/

echo "building test..."
go build -o bin/telesrv ./cmd/telesrv/

echo "build done: bin/telesrv, bin/telesrv-admin"

systemctl start xiro
systemctl start xiro-adm
systemctl start xiro-bot
