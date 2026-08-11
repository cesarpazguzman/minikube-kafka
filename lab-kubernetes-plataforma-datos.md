# Lab práctico: Kubernetes para plataformas de datos
### Una mañana (~4h) — de cero a entender EKS, Helm, Strimzi, Spark Operator y ArgoCD

---

## Objetivo

Al terminar esta mañana debes ser capaz de responder en una entrevista, con soltura y sin dudar:

- Qué es un Pod, Deployment, Service, Job y CronJob, y a qué equivalen en ECS
- Qué hace realmente Helm y por qué no es lo mismo que Terraform
- Qué es un CRD y un Operator, y por qué son la razón por la que las plataformas de datos usan Kubernetes
- Qué es GitOps y en qué se diferencia de un `kubectl apply` desde GitHub Actions
- Cómo se despliega un job de Spark Streaming sobre Kubernetes

**No** es objetivo montar nada production-ready. Es objetivo construir intuición.

---

## Prerequisitos (instalar antes de empezar el cronómetro)

```bash
# macOS
brew install kind kubectl helm k9s
# Docker Desktop debe estar corriendo

# Linux
# kind:
[ $(uname -m) = x86_64 ] && curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind
# kubectl:
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl && sudo mv kubectl /usr/local/bin/
# helm:
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Recursos recomendados para Docker Desktop: **6 CPU / 10 GB RAM**. Kafka y Spark juntos necesitan holgura.

`k9s` es una TUI para navegar el clúster. No es imprescindible, pero acelera muchísimo la comprensión visual de qué está pasando.

---

## Bloque 0 — Docker vs Kubernetes (15 min)

Antes de tocar K8s, fija la distinción con un experimento mínimo.

```bash
docker run -d --name nginx-solo -p 8080:80 nginx:alpine
curl localhost:8080
docker rm -f nginx-solo
curl localhost:8080   # muerto, nadie lo resucita
```

**Anota la conclusión:** Docker ejecuta un contenedor en *una* máquina y no tiene opinión sobre qué pasa si muere. Kubernetes es el orquestador que mantiene un *estado deseado* sobre un *conjunto* de máquinas.

---

## Bloque 1 — Levantar el clúster y los objetos básicos (45 min)

### 1.1 Crear el clúster

`kind` (Kubernetes IN Docker) levanta un clúster real donde cada "nodo" es un contenedor Docker.

```yaml
# kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 30080
      - containerPort: 30081
        hostPort: 30081
  - role: worker
  - role: worker
```

```bash
kind create cluster --name data-lab --config kind-config.yaml
kubectl get nodes           # 3 nodos
docker ps                   # ...que son 3 contenedores Docker. Que esto te haga clic.
kubectl get pods -A         # el control plane corre como pods dentro del propio clúster
```

### 1.2 Tu primer Deployment y Service

```yaml
# 01-app.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: lab
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: lab
spec:
  replicas: 3
  selector:
    matchLabels: { app: web }
  template:
    metadata:
      labels: { app: web }
    spec:
      containers:
        - name: nginx
          image: nginx:alpine
          ports: [{ containerPort: 80 }]
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits:   { cpu: 200m, memory: 128Mi }
---
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: lab
spec:
  type: NodePort
  selector: { app: web }
  ports:
    - port: 80
      targetPort: 80
      nodePort: 30080
```

```bash
kubectl apply -f 01-app.yaml
kubectl get pods -n lab -o wide     # fíjate en qué NODO cae cada pod: eso es el scheduler
curl localhost:30080
```

### 1.3 El experimento que explica Kubernetes

```bash
# Borra un pod a mano
kubectl delete pod -n lab <nombre-de-un-pod>
kubectl get pods -n lab -w
```

Aparece otro en segundos. **Nadie lo ha pedido.** El controller de Deployment vio que el estado real (2 réplicas) no coincidía con el deseado (3) y actuó. Ese bucle de reconciliación es la idea central de Kubernetes, y es también la idea que hay detrás de ArgoCD y de los Operators.

```bash
# Escalado y rolling update
kubectl scale deployment web -n lab --replicas=5
kubectl set image deployment/web nginx=nginx:1.27-alpine -n lab
kubectl rollout status deployment/web -n lab
kubectl rollout undo deployment/web -n lab
```

### 1.4 Job y CronJob (el equivalente a tus tareas de datos)

```yaml
# 02-jobs.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: etl-once
  namespace: lab
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: etl
          image: python:3.12-slim
          command: ["python", "-c"]
          args: ["import time; print('extrayendo...'); time.sleep(10); print('done')"]
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: compactacion
  namespace: lab
spec:
  schedule: "*/2 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: compact
              image: python:3.12-slim
              command: ["python", "-c", "print('rewrite_data_files()')"]
```

```bash
kubectl apply -f 02-jobs.yaml
kubectl logs -n lab job/etl-once
kubectl get cronjob,job -n lab
```

> **Conecta con la entrevista:** esto es exactamente lo que hace el `KubernetesPodOperator` de Airflow — crear un Pod por tarea, esperar a que termine y recoger los logs. Y el CronJob es el patrón del DAG de compactación de Iceberg del que hablamos.

### 1.5 ConfigMap y Secret

```bash
kubectl create configmap app-config -n lab --from-literal=ENV=dev --from-literal=REGION=eu-west-1
kubectl create secret generic db-creds -n lab --from-literal=password=supersecreto
kubectl get secret db-creds -n lab -o jsonpath='{.data.password}' | base64 -d
```

**Anota esto:** los Secrets de K8s van en base64, *no* cifrados por defecto. Por eso en AWS real se usa **IRSA / Pod Identity** (credenciales temporales vía OIDC, sin secretos estáticos) o External Secrets Operator con Secrets Manager. Es una pregunta muy probable en entrevista.

---

## Bloque 2 — Helm (30 min)

### 2.1 Instalar algo con Helm

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm install pg bitnami/postgresql -n lab \
  --set auth.postgresPassword=lab123 \
  --set primary.persistence.size=1Gi

helm list -n lab
kubectl get all -n lab
```

Un solo comando ha creado StatefulSet, Service, Secret, ConfigMap y PVC.

### 2.2 Ver qué hace Helm por dentro

```bash
# Renderiza el YAML sin aplicarlo: ESTA es la orden que hay que interiorizar
helm template pg bitnami/postgresql --set auth.postgresPassword=lab123 | less

# Ver los valores por defecto que puedes sobrescribir
helm show values bitnami/postgresql | less

# Upgrade y rollback versionados
helm upgrade pg bitnami/postgresql -n lab --set primary.persistence.size=2Gi
helm history pg -n lab
helm rollback pg 1 -n lab
```

**Conclusión a anotar:** Helm es una plantilla de YAML + un gestor de releases versionadas. No sustituye al YAML: lo genera. Y no compite con Terraform: Terraform crea el clúster y la infra de AWS; Helm instala cosas *dentro* del clúster.

---

## Bloque 3 — Operators y CRDs: Kafka con Strimzi (60 min)

Este es el bloque que más te va a servir en la entrevista.

### 3.1 Instalar el operator

```bash
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi strimzi/strimzi-kafka-operator -n kafka

# Mira lo que acaba de aparecer en la API del clúster:
kubectl get crd | grep kafka
```

Han aparecido tipos nuevos: `Kafka`, `KafkaTopic`, `KafkaConnect`, `KafkaConnector`, `KafkaNodePool`. **Kubernetes no sabía qué era Kafka hace 30 segundos.** Eso es un CRD: extender la API del clúster con tus propios tipos.

### 3.2 Declarar un clúster de Kafka

```yaml
# 03-kafka.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: lab-cluster
spec:
  replicas: 3
  roles: [controller, broker]
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 5Gi
        deleteClaim: true
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: lab-cluster
  namespace: kafka
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 3.8.0
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: transactions
  namespace: kafka
  labels:
    strimzi.io/cluster: lab-cluster
spec:
  partitions: 6
  replicas: 3
  config:
    retention.ms: 604800000
    cleanup.policy: delete
```

```bash
kubectl apply -f 03-kafka.yaml
kubectl get kafka,pods -n kafka -w    # tarda 3-5 min
```

**Fíjate en `min.insync.replicas: 2` y `default.replication.factor: 3`.** Es exactamente la conversación de RPO≈0 que tuvimos: con `acks=all`, el producer no recibe confirmación hasta que 2 réplicas tienen el mensaje en su log.

### 3.3 Producir y consumir

```bash
# Terminal 1 — consumidor
kubectl -n kafka run consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 \
  --rm=true --restart=Never -- bin/kafka-console-consumer.sh \
  --bootstrap-server lab-cluster-kafka-bootstrap:9092 --topic transactions --from-beginning

# Terminal 2 — productor
kubectl -n kafka run producer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 \
  --rm=true --restart=Never -- bin/kafka-console-producer.sh \
  --bootstrap-server lab-cluster-kafka-bootstrap:9092 --topic transactions
```

### 3.4 El experimento clave: matar un broker

```bash
kubectl delete pod lab-cluster-dual-role-1 -n kafka
kubectl get pods -n kafka -w
```

El operator lo reconstruye, el pod se reincorpora al clúster y las réplicas se re-sincronizan. **Tú no has hecho nada.** Esta es la respuesta a "¿por qué correr Kafka en Kubernetes y no en EC2?".

> **Frase para la entrevista:** *"Un Operator codifica el conocimiento operacional de un sistema con estado — recuperación, rebalanceo, upgrades progresivos — en un controlador que reconcilia continuamente. Es la razón principal por la que las plataformas de datos viven en Kubernetes."*

---

## Bloque 4 — Spark sobre Kubernetes (45 min)

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark --create-namespace --set webhook.enable=true

kubectl get crd | grep spark
```

```yaml
# 04-spark.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: spark
spec:
  type: Scala
  mode: cluster
  image: spark:3.5.3
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples.jar"
  sparkVersion: "3.5.3"
  restartPolicy:
    type: Never          # batch. Para streaming sería: Always
  driver:
    cores: 1
    memory: 1g
    serviceAccount: spark-operator-spark
  executor:
    cores: 1
    instances: 2
    memory: 1g
```

```bash
kubectl apply -f 04-spark.yaml
kubectl get sparkapplication,pods -n spark -w
kubectl logs -n spark spark-pi-driver | grep "Pi is roughly"
```

**Observa el ciclo completo:** aparece el pod driver → el driver crea los pods executor → los executors terminan y desaparecen → el driver termina en estado `Completed`. Eso es Spark en modo cluster sobre K8s.

Anota las diferencias que importan para tu caso real:
- `restartPolicy: Always` para streaming (un job de streaming nunca "termina"; si muere, debe volver)
- El `serviceAccount` es el punto de anclaje de **IRSA** en EKS → así accede el job a S3 sin credenciales estáticas
- Un job de streaming **no lo orquesta Airflow**: vive permanentemente en el clúster. Airflow orquesta cosas con principio y fin

---

## Bloque 5 — GitOps con ArgoCD (45 min)

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

# Contraseña inicial
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# UI
kubectl port-forward svc/argocd-server -n argocd 8080:443
# → https://localhost:8080  (usuario: admin)
```

### 5.1 Crear una Application

Sube tus manifiestos (`01-app.yaml`, `02-jobs.yaml`) a un repo Git público tuyo, en una carpeta `manifests/`.

```yaml
# 05-argo-app.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: lab-app
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/TU_USUARIO/TU_REPO.git
    targetRevision: main
    path: manifests
  destination:
    server: https://kubernetes.default.svc
    namespace: lab
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

```bash
kubectl apply -f 05-argo-app.yaml
```

### 5.2 Los dos experimentos que explican GitOps

**Auto-heal:**
```bash
kubectl scale deployment web -n lab --replicas=1
# Mira la UI de Argo: detecta OutOfSync y lo revierte a 3 automáticamente
```

**Despliegue por Git:**
```bash
# Cambia replicas: 3 → 4 en el repo, commit y push
# Sin tocar el clúster, Argo lo aplica en <3 min (o pulsa Refresh)
```

**Anota la conclusión:** GitHub Actions nunca necesitó credenciales del clúster. El clúster tira de Git, no al revés. Eso es el modelo **pull** frente al **push**, y es la diferencia entre GitOps y un `kubectl apply` desde CI.

---

## Limpieza

```bash
kind delete cluster --name data-lab
```

---

## Entregable: notas para la entrevista

Antes de cerrar el portátil, escribe **de memoria** (sin mirar) tus respuestas a esto. Si alguna no te sale, vuelve al bloque correspondiente:

1. Diferencia entre imagen Docker, contenedor y Kubernetes — en tres frases
2. Tabla de equivalencias ECS → K8s (Task Def, Task, Service, Scheduled Task, Task Role)
3. Qué es el bucle de reconciliación y dónde lo has visto actuar hoy (tres sitios distintos)
4. Qué es un CRD y un Operator, con Strimzi como ejemplo concreto
5. Helm vs Terraform: dónde termina uno y empieza el otro
6. Push vs pull deployment: tres ventajas concretas de GitOps
7. Cómo accede tu job de Spark a S3 en EKS sin credenciales estáticas
8. Por qué `restartPolicy` difiere entre un job batch y uno de streaming
9. Cómo se relaciona `min.insync.replicas=2` + `acks=all` con el RPO
10. Por qué un job de Spark Streaming no se orquesta con Airflow

---

## Si te sobra tiempo (opcional)

- **Observabilidad:** `helm install kube-prometheus-stack` y mira las métricas que Strimzi expone en Prometheus. Conecta directamente con el requisito de observabilidad de la oferta.
- **Airflow en K8s:** `helm install apache-airflow/airflow` con `executor=KubernetesExecutor` y lanza un DAG con `KubernetesPodOperator`. (En el stack real usaríais MWAA, pero ver el executor funcionando explica el modelo.)
- **Terraform + Helm:** replica el bloque 2 usando el provider `helm` de Terraform contra el clúster kind. Te prepara para la conversación de la separación de states.

---

## Realismo sobre el tiempo

Bloques 0-3 son el núcleo: **con eso ya puedes defender la conversación en la entrevista.** Si la mañana se acaba ahí, has ganado. Los bloques 4 y 5 son los que más impresionan, pero son también los que puedes explicar razonablemente bien sin haberlos ejecutado.

Si algo falla y te bloquea más de 15 minutos, sáltalo y sigue. El objetivo es intuición, no un clúster perfecto.
