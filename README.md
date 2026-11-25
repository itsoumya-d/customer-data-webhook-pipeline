# Customer Data Webhook Pipeline

A lightweight, high-reliability event ingestion and webhook dispatch engine designed for customer data platforms (CDP) and marketing automation systems.

## Key Capabilities
- **Event Ingestion & Normalization**: Validates customer identification keys, event attributes, and timestamps.
- **Deduplication & Idempotency**: Suppresses duplicate webhook payloads using cryptographic SHA-256 fingerprinting.
- **Resilient Dispatch**: Transactional event dispatch with automated exponential backoff retries across transient downstream webhook failures.

## Test & Build
```bash
npm install
npm test
npm run build
```
