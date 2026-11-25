import { describe, it, expect, beforeEach } from "vitest";
import { CustomerDataPipeline } from "../src/pipeline.js";

describe("CustomerDataPipeline", () => {
  let pipeline: CustomerDataPipeline;

  beforeEach(() => {
    pipeline = new CustomerDataPipeline();
  });

  it("normalizes valid customer event attributes", () => {
    const raw = {
      userId: "usr_42",
      event: "product_purchased",
      properties: { item: "book", price: 29.99 },
    };
    const normalized = pipeline.normalize(raw);

    expect(normalized.userId).toBe("usr_42");
    expect(normalized.event).toBe("product_purchased");
    expect(normalized.properties.price).toBe(29.99);
    expect(normalized.timestamp).toBeGreaterThan(0);
    expect(normalized.messageId).toMatch(/^msg_/);
  });

  it("rejects events missing customer identifiers or event name", () => {
    expect(() => pipeline.normalize({ event: "login" })).toThrow(
      "must include at least userId or anonymousId"
    );
    expect(() => pipeline.normalize({ userId: "123", event: "" })).toThrow(
      "must include a non-empty event name"
    );
  });

  it("computes reproducible fingerprints and deduplicates duplicate payloads", async () => {
    const event = {
      userId: "usr_100",
      event: "checkout_completed",
      properties: { cartId: "c_1" },
      timestamp: 1700000000000,
    };

    const delivery = {
      destinationUrl: "https://api.destination.internal/webhook",
      secretKey: "supersecret",
    };

    const first = await pipeline.processAndDispatch(event, delivery);
    expect(first.success).toBe(true);
    expect(first.attempts).toBe(1);

    const second = await pipeline.processAndDispatch(event, delivery);
    expect(second.success).toBe(true);
    expect(second.attempts).toBe(0);
    expect(second.error).toContain("Deduplicated");

    expect(pipeline.getStats().deduplicated).toBe(1);
    expect(pipeline.getStats().dispatched).toBe(1);
  });

  it("generates deterministic HMAC signatures for payload integrity", () => {
    const event = pipeline.normalize({
      userId: "usr_sign",
      event: "subscription_renewed",
      timestamp: 1700000000000,
      messageId: "msg_fixed",
    });

    const sig1 = pipeline.signPayload(event, "key-alpha");
    const sig2 = pipeline.signPayload(event, "key-alpha");
    const sigOtherKey = pipeline.signPayload(event, "key-beta");

    expect(sig1).toBe(sig2);
    expect(sig1).not.toBe(sigOtherKey);
  });

  it("retries upon transient downstream failure and succeeds on retry", async () => {
    let callCount = 0;
    const senderMock = async () => {
      callCount++;
      if (callCount < 2) {
        throw new Error("Connection timeout");
      }
      return { status: 200 };
    };

    const event = {
      userId: "usr_retry",
      event: "push_opened",
    };

    const result = await pipeline.processAndDispatch(
      event,
      { destinationUrl: "https://webhook.internal/ingest", secretKey: "key" },
      senderMock
    );

    expect(result.success).toBe(true);
    expect(result.attempts).toBe(2);
    expect(pipeline.getStats().dispatched).toBe(1);
    expect(pipeline.getStats().failed).toBe(0);
  });

  it("records failed status when all retry attempts fail", async () => {
    const senderMock = async () => {
      throw new Error("Downstream 503 Service Unavailable");
    };

    const event = {
      userId: "usr_fail",
      event: "app_crashed",
    };

    const result = await pipeline.processAndDispatch(
      event,
      { destinationUrl: "https://webhook.internal/ingest", secretKey: "key", maxRetries: 3 },
      senderMock
    );

    expect(result.success).toBe(false);
    expect(result.attempts).toBe(3);
    expect(pipeline.getStats().failed).toBe(1);
  });
});
