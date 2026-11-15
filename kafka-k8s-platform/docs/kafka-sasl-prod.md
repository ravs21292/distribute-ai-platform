"""Docs: Kafka SASL/TLS for production overlays (not enabled on Kind PLAINTEXT demo).

Kind + Compose use PLAINTEXT on host.docker.internal:19092.
For managed Kafka (MSK / Confluent), set in Secret / overlay:

  DAP_KAFKA_BOOTSTRAP_SERVERS=b-1.example.kafka.amazonaws.com:9096
  DAP_KAFKA_SASL_USERNAME=...
  DAP_KAFKA_SASL_PASSWORD=...
  # Wire SASL_SSL in client code when you enable a prod security profile.

Do not change the local Kind demo listeners unless you also update KEDA bootstrapServers.
"""
