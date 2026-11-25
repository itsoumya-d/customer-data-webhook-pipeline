export interface CustomerEvent {
  userId?: string;
  anonymousId?: string;
  event: string;
  properties: Record<string, unknown>;
  timestamp: number;
  messageId: string;
}

export interface WebhookDeliveryRequest {
  destinationUrl: string;
  secretKey: string;
  maxRetries?: number;
}

export interface DispatchResult {
  eventId: string;
  success: boolean;
  statusCode: number;
  attempts: number;
  durationMs: number;
  error?: string;
}

export interface PipelineStats {
  processed: number;
  deduplicated: number;
  dispatched: number;
  failed: number;
}
