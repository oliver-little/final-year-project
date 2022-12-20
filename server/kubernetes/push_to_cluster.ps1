docker tag worker localhost:5000/worker
docker push localhost:5000/worker
docker tag orchestrator localhost:5000/orchestrator
docker push localhost:5000/orchestrator
