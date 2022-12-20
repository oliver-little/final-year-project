kubectl delete -f .\configs\
helm delete cassandra-release --set service.type=NodePort