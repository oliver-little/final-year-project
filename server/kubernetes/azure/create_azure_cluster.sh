helm repo add jetstack https://charts.jetstack.io
helm repo add k8ssandra https://helm.k8ssandra.io/
helm repo update
helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set installCRDs=true
helm install k8ssandra-operator k8ssandra/k8ssandra-operator -n k8ssandra-operator --create-namespace
kubectl apply -n k8ssandra-operator -f demo_cluster.yaml

# For checking the cluster is up
# kubectl -n k8ssandra-operator get pods
# kubectl describe k8ssandracluster demo -n k8ssandra-operator