kind create cluster --config=cluster_configs/kind.yaml
kubectl config use-context kind-kind
helm repo add jetstack https://charts.jetstack.io
helm repo add k8ssandra https://helm.k8ssandra.io/
helm repo update
helm install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set installCRDs=true
helm install k8ssandra-operator k8ssandra/k8ssandra-operator -n k8ssandra-operator --set global.clusterScoped=true --create-namespace
# Wait until this both containers in this command are running:
# kubectl get pods -n k8ssandra-operator
kubectl apply -n k8ssandra-operator -f cluster_configs/k8ssandra_test.yaml
# Wait until all this describe command is ready:
# kubectl describe k8cs test -n k8ssandra-operator
# Just port-forward to a node in cassandra
Start-Process -NoNewWindow kubectl "port-forward svc/test-dc1-all-pods-service -n k8ssandra-operator 9042"
# AlternativelyPort forward to Stargate service
Start-Process -NoNewWindow kubectl "port-forward svc/test-dc1-stargate-service -n k8ssandra-operator 8080 8081 8082 9042"

# When ready, stop the port-forward and container
Stop-Process -Name kubectl

# Username and password
$CASS_USERNAME=$([System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($(kubectl get secret test-superuser -n k8ssandra-operator -o=jsonpath='{.data.username}'))))
$CASS_USERNAME
$CASS_PASSWORD=$([System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($(kubectl get secret test-superuser -n k8ssandra-operator -o=jsonpath='{.data.password}'))))
$CASS_PASSWORD
