.PHONY: build clean remove deploy_dev deploy_prod

build:
	env GOARCH=amd64 GOOS=linux go build -ldflags="-s -w" -o bin/stt stt/main.go
	#env GOARCH=amd64 GOOS=linux go build -ldflags="-s -w" -o bin/tts tts/main.go

clean:
	rm -rf ./bin

remove_dev:
	sls remove --stage dev

remove_prod:
	sls remove --stage prod

deploy_dev: clean build
	sls deploy --verbose --stage dev --aws-profile default

deploy_prod: clean build
	sls deploy --verbose --stage prod --aws-profile default
