export interface User {
  id: string;
  email: string;
  name: string;
  role: 'admin' | 'user';
  createdAt: string;
  updatedAt: string;
}

export interface LoginRequest {
  email: string;
  password: string;
}

export interface LoginResponse {
  token: string;
  user: User;
}

export interface StatsSummary {
  totalPipelines: number;
  activePipelines: number;
  totalEventsProcessed: number;
  eventsPerSecond: number;
  lagMs: number;
  errorsLast24h: number;
}

export interface Pipeline {
  id: string;
  name: string;
  description?: string;
  status: PipelineStatus;
  source: Source;
  sink: Sink;
  processorConfig: ProcessorConfig;
  globalConfig: GlobalConfig;
  createdAt: string;
  updatedAt: string;
  lastRunAt?: string;
}

export type PipelineStatus = 'running' | 'stopped' | 'error' | 'paused';

export interface ProcessorConfig {
  batchSize: number;
  flushIntervalMs: number;
  maxRetries: number;
  retryDelayMs: number;
  deadLetterQueueEnabled: boolean;
  transformations?: Transformation[];
}

export interface Transformation {
  id: string;
  type: 'filter' | 'map' | 'enrich';
  config: Record<string, unknown>;
  order: number;
}

export interface TableStats {
  tableName: string;
  eventsInserted: number;
  eventsUpdated: number;
  eventsDeleted: number;
  eventsTotal: number;
  lagMs: number;
  lastEventAt?: string;
}

export interface Source {
  id: string;
  type: 'postgres' | 'mysql' | 'mongodb';
  name: string;
  connection: {
    host: string;
    port: number;
    database: string;
    username: string;
    password?: string;
    ssl?: boolean;
  };
  tables: string[];
}

export interface Sink {
  id: string;
  type: 'postgres' | 'mysql' | 'elasticsearch' | 'kafka' | 'webhook';
  name: string;
  connection: {
    host: string;
    port?: number;
    index?: string;
    topic?: string;
    url?: string;
    headers?: Record<string, string>;
  };
}

export interface GlobalConfig {
  maxConcurrentPipelines: number;
  defaultBatchSize: number;
  defaultFlushIntervalMs: number;
  healthCheckIntervalMs: number;
  metricsRetentionDays: number;
}

export interface WorkerHeartbeat {
  workerId: string;
  pipelineId: string;
  status: 'healthy' | 'unhealthy' | 'busy';
  lastHeartbeatAt: string;
  eventsProcessed: number;
  currentLagMs: number;
  memoryUsageMb: number;
  cpuUsagePercent: number;
}

export interface SSEMetrics {
  type: 'metrics' | 'heartbeat' | 'error' | 'status_change';
  timestamp: string;
  data: {
    pipelineId?: string;
    eventsPerSecond?: number;
    lagMs?: number;
    errorCount?: number;
    message?: string;
    status?: PipelineStatus;
  };
}
