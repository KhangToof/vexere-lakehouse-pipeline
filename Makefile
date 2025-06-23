up:
	docker compose up -d

down:
	docker compose down 

restart:
	make down 
	make build
	make up

up_meta:
	docker compose -f ../metabase/docker-compose.yml up -d

down_meta:
	docker compose -f ../metabase/docker-compose.yml down

build:
	docker compose build

host:
	ngrok http 9000 --url helped-polite-snake.ngrok-free.app