include .env

ifeq ($(SERVER),)
    $(error SERVER env is not set)
endif

NGINX_CONF:=/etc/nginx
MYSQL_CONF:=/etc/mysql

APP:=/home/isucon/webapp/go
APP_BINARY:=isupipe

SERVICE:=isupipe-go.service


.PHONY: dump-all
dump-all: dump-nginx dump-mysql

.PHONY: dump-nginx
dump-nginx:
	echo "dump nginx conf"
	mkdir -p ./$(SERVER)/etc
	cp -r $(NGINX_CONF) ./$(SERVER)/etc

.PHONY: dump-mysql
dump-mysql:
	echo "dump nginx conf"
	mkdir -p ./$(SERVER)/etc
	cp -r $(MYSQL_CONF) ./$(SERVER)/etc




.PHONY: deploy-all
deploy-all: conf-deploy app-deploy

.PHONY: conf-deploy
conf-deploy: nginx-conf-deploy mysql-conf-deploy

.PHONY: nginx-conf-deploy
nginx-conf-deploy:
	echo "nginx conf deploy"
	if [ ! -d $(SERVER)/etc/nginx ]; then echo "nginx not configured"; exit 1; fi
	sudo cp -r $(SERVER)/etc/nginx/* $(NGINX_CONF)
	sudo nginx -t
	sudo systemctl restart nginx

.PHONY: mysql-conf-deploy
mysql-conf-deploy:
	echo "mysql conf deploy"
	if [ ! -d $(SERVER)/etc/mysql ]; then echo "mysql not configured"; exit 1; fi
	sudo cp -r $(SERVER)/etc/mysql/* $(MYSQL_CONF)
	sudo systemctl restart mysql

.PHONY: app-deploy
app-deploy:
	echo "app deploy"
	cd $(APP) && go build -o $(APP_BINARY) *.go
	sudo systemctl restart $(SERVICE)

.PHONY: link
link:
	echo "link"
	rm -f /home/isucon/env.sh
	sudo ln $(SERVER)/env.sh /home/isucon/env.sh
