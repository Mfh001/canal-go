
docker stop canal-cli
docker rm canal-cli
docker rmi -f  `docker images | grep 'canal-cli' | awk '{print $3}'`

docker build -t=canal-cli:1.0 .

docker run -itd --name canal-cli --net=host canal-cli:1.0 ./main