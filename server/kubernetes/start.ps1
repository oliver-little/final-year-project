kubectl apply -f .\configs\
helm install cassandra-release bitnami/cassandra --set service.type=NodePort --set dbUser.forcePassword=false