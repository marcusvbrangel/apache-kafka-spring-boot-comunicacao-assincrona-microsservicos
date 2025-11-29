# Configuração do Kafka para Desenvolvimento Local

## Problema Resolvido ✅

### Raiz do Problema
O Kafka estava rodando em um container Docker, mas as aplicações Spring Boot (shop-api e validator-api) estavam rodando localmente na sua máquina. Isso causava um isolamento de rede onde as aplicações não conseguiam alcançar o Kafka.

### Erro Específico
```
java.net.UnknownHostException: kafka
Error connecting to node kafka:9092
```

A aplicação localmente estava recebendo `kafka:9092` do Kafka, mas não conseguia resolver esse nome (é um nome interno do Docker). Ela deveria receber `localhost:9092`.

## Solução Implementada

### 1. Docker Compose Atualizado

O `docker-compose.yaml` foi configurado para expor o Kafka em múltiplos listeners:

```yaml
# PLAINTEXT é para comunicação dentro da rede Docker (containers)
# PLAINTEXT_HOST é para acesso de localhost (suas aplicações Spring Boot)
KAFKA_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://kafka:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

**Ports Expostos:**
- **9092:9092** - Porta PLAINTEXT (containers dentro da rede Docker)
- **29092:29092** - Porta PLAINTEXT_HOST (acesso do localhost)

**Como Funciona:**
- **PLAINTEXT://kafka:9092** - Listener que escuta no nome `kafka` (dentro da rede Docker) - porta 9092
- **PLAINTEXT_HOST://kafka:29092** - Listener que escuta no host `kafka` na porta 29092 (external)
- **Advertised para localhost:29092** - Quando alguém de fora (sua aplicação local) se conecta em localhost:29092, recebe `localhost:29092` como resposta
- **Advertised para kafka:9092** - Quando um container se conecta, recebe `kafka:9092` como resposta
- A aplicação conecta em `localhost:29092` e o Kafka retorna o mesmo endereço para reconexões

## Como Rodar

### Passo 1: Iniciar Kafka, Zookeeper e Redpanda Console

```bash
docker compose up -d
```

Isso vai iniciar:
- **Zookeeper** (porta 2181): coordenador do Kafka
- **Kafka** (porta 9092): broker Kafka
- **Redpanda Console** (porta 8500): UI para monitorar tópicos

### Passo 2: Iniciar suas aplicações Spring Boot

Abra dois terminais diferentes e rode:

**Terminal 1 - shop-api:**
```bash
cd shop-api
./mvnw spring-boot:run
```

**Terminal 2 - validator-api:**
```bash
cd validator-api
./mvnw spring-boot:run
```

### Passo 3: Testar a comunicação

1. Acesse o Redpanda Console em http://localhost:8500
2. Crie um pedido via POST em http://localhost:8051/shop
3. Veja se o validator-api recebe a mensagem
4. Verifique se o status é atualizado em shop-api

## Fluxo de Mensagens

```
1. POST /shop em shop-api:8051
   ↓
2. shop-api publica em SHOP_TOPIC (localhost:29092)
   ↓
3. validator-api consome de SHOP_TOPIC
   ↓
4. validator-api valida e publica resultado em SHOP_TOPIC_EVENT (localhost:29092)
   ↓
5. shop-api consome de SHOP_TOPIC_EVENT
   ↓
6. shop-api atualiza status do pedido
```

## Variáveis de Ambiente

Se precisar usar endereços diferentes, exporte as variáveis antes de rodar:

```bash
# Para localhost (padrão)
export KAFKA_BOOTSTRAP_ADDRESS=localhost:29092

# Se rodar aplicações em containers (não é seu caso)
export KAFKA_BOOTSTRAP_ADDRESS=kafka:9092
```

## Monitorando Mensagens

### Via Redpanda Console (mais fácil)
Acesse http://localhost:8500 e navegue para "Topics"

### Via CLI
```bash
# Listar tópicos
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consumir mensagens de um tópico
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic SHOP_TOPIC --from-beginning

# Consumir mensagens de eventos
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic SHOP_TOPIC_EVENT --from-beginning
```

## Problema: Se ainda não funcionar

Verifique:

1. **Kafka está rodando?**
   ```bash
   docker ps | grep kafka
   ```

2. **Porta 9092 está aberta?**
   ```bash
   netstat -an | grep 9092
   ```

3. **Aplicações conectando?** (veja logs para "Connecting to Kafka")
   ```bash
   # No seu terminal onde rodou shop-api
   # Procure por logs de conexão bem-sucedida ao Kafka
   ```

4. **Tópicos existem?**
   ```bash
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic SHOP_TOPIC
   ```

## Ports Usadas

| Serviço | Porta | Interno | Uso |
|---------|-------|---------|-----|
| Kafka | 9092 | - | Conexão de aplicações locais |
| Zookeeper | 2181 | - | Coordenação Kafka |
| Redpanda Console | 8500 | 8080 | UI de Monitoramento |

## Arquivos Modificados

- `docker-compose.yaml` - Adicionadas configurações de múltiplos listeners
- `shop-api/src/main/resources/application.properties` - Comentários explicativos
- `validator-api/src/main/resources/application.properties` - Comentários explicativos

## Próximos Passos (Opcional)

Para produção, você pode:
1. Criar Dockerfiles para suas aplicações
2. Adicionar as aplicações ao docker-compose
3. Usar variáveis de ambiente para configurar endereços dinamicamente