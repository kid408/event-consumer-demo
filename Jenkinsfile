pipeline {
  agent any

  options {
    skipDefaultCheckout(true)
  }

  environment {
    NOMAD_ADDR = 'http://127.0.0.1:4646'
    CONSUL_ADDR = 'http://127.0.0.1:8500'
    IMAGE_TAG = "${env.BUILD_NUMBER ?: 'dev'}"
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Check Docker') {
      steps {
        sh '''
          set -eu
          docker version
          docker buildx version
          docker buildx ls
          nomad version
        '''
      }
    }

    stage('Preflight') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"

          echo '=== active nomad processes ==='
          ps -ef | grep '[n]omad' || true

          echo '=== consul leader ==='
          curl -fsS "${CONSUL_ADDR}/v1/status/leader"
          echo

          echo '=== nomad leader ==='
          curl -fsS "${NOMAD_ADDR}/v1/status/leader"
          echo

          echo '=== nomad node status ==='
          nomad node status

          READY_NODE="$(nomad node status -json | jq -r 'map(select(.Status=="ready"))[0].ID // empty')"
          test -n "${READY_NODE}"
          NODE_DC="$(nomad node status -json | jq -r 'map(select(.Status=="ready"))[0].Datacenter // empty')"
          test -n "${NODE_DC}"
          JOB_DC="$(sed -n 's/^datacenters[[:space:]]*=[[:space:]]*\\[\"\\([^\"]*\\)\"\\].*/\\1/p' nomad/event-consumer.vars.hcl)"
          test -n "${JOB_DC}"
          test "${NODE_DC}" = "${JOB_DC}"

          echo "=== ready node: ${READY_NODE} ==="
          nomad node status -verbose "${READY_NODE}" | tee /tmp/nomad-event-consumer-demo-node.txt

          grep -Eq '^[[:space:]]*logs[[:space:]]' /tmp/nomad-event-consumer-demo-node.txt
        '''
      }
    }

    stage('Build Image') {
      steps {
        sh '''
          set -eu
          docker buildx build \
            --platform linux/amd64 \
            --provenance=false \
            --load \
            -f Dockerfile \
            -t event-consumer-demo:${IMAGE_TAG} \
            .
          docker tag event-consumer-demo:${IMAGE_TAG} event-consumer-demo:dev
          docker image inspect event-consumer-demo:${IMAGE_TAG} >/dev/null
          docker image inspect event-consumer-demo:dev >/dev/null
        '''
      }
    }

    stage('Deploy') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"
          docker rm -f event-consumer-demo || true
          nomad job run -detach \
            -var-file=nomad/event-consumer.vars.hcl \
            -var "image=event-consumer-demo:${IMAGE_TAG}" \
            nomad/event-consumer.nomad.hcl
        '''
      }
    }

    stage('Smoke Test') {
      steps {
        sh '''
          set -eu
          export NOMAD_ADDR="${NOMAD_ADDR}"

          diagnose() {
            echo '=== nomad node status ==='
            nomad node status || true
            echo '=== nomad job status ==='
            nomad job status -verbose event-consumer-demo || true
            echo '=== nomad job allocations ==='
            nomad job allocs event-consumer-demo || true
            echo '=== consul event-consumer-demo-http ==='
            curl -fsS "${CONSUL_ADDR}/v1/health/service/event-consumer-demo-http?passing=true" | jq . || true
            echo '=== consul event-consumer-demo-prom ==='
            curl -fsS "${CONSUL_ADDR}/v1/health/service/event-consumer-demo-prom?passing=true" | jq . || true
          }

          trap 'diagnose' 0

          printf '[]\n' > /tmp/event-consumer-demo-http.json
          printf '[]\n' > /tmp/event-consumer-demo-prom.json

          for _ in $(seq 1 30); do
            curl -fsS "${CONSUL_ADDR}/v1/health/service/event-consumer-demo-http?passing=true" > /tmp/event-consumer-demo-http.json || printf '[]\n' > /tmp/event-consumer-demo-http.json
            curl -fsS "${CONSUL_ADDR}/v1/health/service/event-consumer-demo-prom?passing=true" > /tmp/event-consumer-demo-prom.json || printf '[]\n' > /tmp/event-consumer-demo-prom.json

            if jq -e 'length > 0' /tmp/event-consumer-demo-http.json >/dev/null 2>&1 &&
               jq -e 'length > 0' /tmp/event-consumer-demo-prom.json >/dev/null 2>&1; then
              nomad job status -verbose event-consumer-demo
              jq . /tmp/event-consumer-demo-http.json
              jq . /tmp/event-consumer-demo-prom.json
              trap - 0
              exit 0
            fi

            sleep 2
          done

          exit 1
        '''
      }
    }
  }
}
