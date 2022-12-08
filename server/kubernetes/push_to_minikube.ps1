# Tag images, push to local registry, pull into minikube from local registry
Start-Process -NoNewWindow kubectl "port-forward --namespace kube-system service/registry 5000:80"
docker run --rm -itd --network=host alpine ash -c "apk add socat && socat TCP-LISTEN:5000,reuseaddr,fork TCP:host.docker.internal:5000"

docker tag worker localhost:5000/worker
docker push localhost:5000/worker
docker tag orchestrator localhost:5000/orchestrator
docker push localhost:5000/orchestrator

# Stop the port-forward and container
Stop-Process -Name kubectl
docker stop $(docker ps -a -q --filter ancestor=alpine) 