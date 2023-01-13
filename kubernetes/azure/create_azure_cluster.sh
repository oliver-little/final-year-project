helm repo add jetstack https://charts.jetstack.io
helm repo add k8ssandra https://helm.k8ssandra.io/
helm repo update
helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set installCRDs=true
helm install k8ssandra-operator k8ssandra/k8ssandra-operator -n k8ssandra-operator --set global.clusterScoped=true --create-namespace
kubectl apply -f demo_cluster.yaml

# For checking the cluster is up
# kubectl get pods
# kubectl describe k8ssandracluster demo    

# Extract username and password
#kubectl get secrets/demo-superuser --template="{{.data.username}}" | base64 -d
#kubectl get secrets/demo-superuser --template="{{.data.password}}" | base64 -d