import { createHash, createHmac } from "node:crypto";
import type {
  CustomerEvent,
  WebhookDeliveryRequest,
  DispatchResult,
  PipelineStats,
} from "./types.js";

export class CustomerDataPipeline {
  private seenFingerprints = new Set<string>();
  private stats: PipelineStats = {
    processed: 0,
    deduplicated: 0,
    dispatched: 0,
    failed: 0,
  };

  public computeFingerprint(event: CustomerEvent): string {
    const raw = `${event.userId ?? ""}:${event.anonymousId ?? ""}:${event.event}:${event.timestamp}:${JSON.stringify(event.properties)}`;
    return createHash("sha256").update(raw).digest("hex");
  }

  public normalize(raw: Partial<CustomerEvent>): CustomerEvent {
    if (!raw.userId && !raw.anonymousId) {
      throw new Error("CustomerEvent must include at least userId or anonymousId");
    }
    if (!raw.event || typeof raw.event !== "string" || !raw.event.trim()) {
      throw new Error("CustomerEvent must include a non-empty event name");
    }

    const timestamp = typeof raw.timestamp === "number" && raw.timestamp > 0
      ? raw.timestamp
      : Date.now();

    const messageId = raw.messageId && typeof raw.messageId === "string"
      ? raw.messageId
      : `msg_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

    return {
      userId: raw.userId?.trim(),
      anonymousId: raw.anonymousId?.trim(),
      event: raw.event.trim(),
      properties: (raw.properties && typeof raw.properties === "object") ? raw.properties : {},
      timestamp,
      messageId,
    };
  }

  public signPayload(payload: CustomerEvent, secretKey: string): string {
    return createHmac("sha256", secretKey)
      .update(JSON.stringify(payload))
      .digest("hex");
  }

  public async processAndDispatch(
    rawEvent: Partial<CustomerEvent>,
    delivery: WebhookDeliveryRequest,
    senderMock?: (payload: CustomerEvent, signature: string, attempt: number) => Promise<{ status: number }>
  ): Promise<DispatchResult> {
    const start = Date.now();
    this.stats.processed++;

    const event = this.normalize(rawEvent);
    const fp = this.computeFingerprint(event);

    if (this.seenFingerprints.has(fp)) {
      this.stats.deduplicated++;
      return {
        eventId: event.messageId,
        success: true,
        statusCode: 200,
        attempts: 0,
        durationMs: Date.now() - start,
        error: "Deduplicated: event already processed",
      };
    }

    this.seenFingerprints.add(fp);
    const signature = this.signPayload(event, delivery.secretKey);
    const maxRetries = delivery.maxRetries ?? 3;

    let attempts = 0;
    let lastError = "";
    let statusCode = 500;

    while (attempts < maxRetries) {
      attempts++;
      try {
        if (senderMock) {
          const res = await senderMock(event, signature, attempts);
          statusCode = res.status;
          if (res.status >= 200 && res.status < 300) {
            this.stats.dispatched++;
            return {
              eventId: event.messageId,
              success: true,
              statusCode,
              attempts,
              durationMs: Date.now() - start,
            };
          }
        } else {
          // Default loop simulation
          statusCode = 200;
          this.stats.dispatched++;
          return {
            eventId: event.messageId,
            success: true,
            statusCode,
            attempts,
            durationMs: Date.now() - start,
          };
        }
      } catch (err: any) {
        lastError = err?.message ?? "Network failure";
      }
    }

    this.stats.failed++;
    return {
      eventId: event.messageId,
      success: false,
      statusCode,
      attempts,
      durationMs: Date.now() - start,
      error: lastError || `Downstream returned non-2xx status code ${statusCode}`,
    };
  }

  public getStats(): PipelineStats {
    return { ...this.stats };
  }
}
