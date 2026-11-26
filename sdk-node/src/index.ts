/**
 * flux SDK para Node.js — Event Protocol v1, nivel de conformidad L2.
 *
 * Especificación: https://github.com/charlessonamericantrading/flux
 */

export {
  connect,
  FluxBus,
  ConsumerConfigMismatchError,
  TenantIsolationError,
} from "./client.js";
export type {
  ConnectOptions,
  PublishOptions,
  SubscribeOptions,
  HandlerContext,
  Handler,
  Subscription,
} from "./client.js";

export {
  NotProvisionedError,
  PermissionDeniedError,
  ConnectionRejectedError,
  InvalidCredentialsError,
  InvalidTlsOptionsError,
} from "./security.js";
export type { Credentials, TlsOptions } from "./security.js";

export {
  ErrorClass,
  RetryableError,
  PermanentError,
  PoisonError,
  isClassifiedError,
} from "./errors.js";
export type { Classification, ClassifiedError } from "./errors.js";

export {
  classify,
  createClassifier,
  createDetailedClassifier,
  RETRYABLE_HTTP_STATUS,
  TRANSIENT_SYSCALL_CODES,
} from "./classify.js";
export type { ClassifierOptions } from "./classify.js";

export {
  buildEvent,
  parseEvent,
  serialize,
  toDlqEvent,
  stripDlqExtensions,
  EnvelopeError,
  ALLOWED_ROOT_ATTRIBUTES,
} from "./envelope.js";
export type { FluxEvent, DataClassification } from "./envelope.js";

export {
  CONSUMER_DEFAULTS,
  STREAM_DEFAULTS,
  MAX_MESSAGE_BYTES,
  SUBJECT_PATTERN,
  InvalidSubjectError,
  InvalidServiceNameError,
  SERVICE_PATTERN,
  parseSubject,
  isValidSubject,
  subjectToType,
  streamName,
  dlqStreamName,
  durableName,
  dlqSubject,
  isDlqSubject,
  sourceUri,
  RESERVED_DOMAINS,
  assertNotReservedDomain,
} from "./protocol.js";
export type { ParsedSubject } from "./protocol.js";

export {
  createValidator,
  schemaUriFor,
  stateSchemaUriFor,
  rpcSchemaUriFor,
  SchemaValidationError,
  SchemaNotFoundError,
} from "./validation.js";
export type { ValidationOptions, ValidationMode, SchemaBundle, Validator } from "./validation.js";

export {
  createSigner,
  createVerifier,
  generateKeyPair,
  SigningKeyError,
} from "./signing.js";
export type { SigningOptions, VerificationMode, Signer, Verifier } from "./signing.js";

export { InMemoryMetrics, NO_METRICS, DURATION_BUCKETS } from "./metrics.js";
export type {
  MetricsSink,
  PublishOutcome,
  ConsumeOutcome,
  ConnectionState,
  StateWriteOutcome,
  RpcClientOutcome,
  RpcServerOutcome,
} from "./metrics.js";

export { currentContext } from "./context.js";
export type { EventContext } from "./context.js";

// ─── Estado replicado — la capacidad `state` (specification/10-state.md) ─────────────────
// `bus.state.put | delete | get | watch`; los siete códigos de 04-errors.md §6; y las funciones
// PURAS y deterministas que construyen los bytes (el arnés cruzado las usa: sin reloj ni UUID).

export {
  // funciones puras
  stateSubject,
  stateStreamName,
  stateDurableName,
  stateFilterSubject,
  stateDlqSubject,
  stateType,
  stateTombstoneType,
  stateDataschema,
  parseStateContract,
  buildStateEvent,
  buildStateTombstone,
  toStateDlqEvent,
  buildStateDlqEvent,
  buildUnreadableStateDlqEvent,
  // validación
  assertStateKey,
  assertStateTenant,
  assertStateClassification,
  aggregateVersionOf,
  isValidAggregateVersion,
  isSequenceConflict,
  // constantes
  STATE_KEY_PATTERN,
  STATE_TENANT_PATTERN,
  STATE_TENANT_MAX_LENGTH,
  STATE_CONTRACT_PATTERN,
  STATE_CONFLICT_CODES,
  STATE_MAX_ATTEMPTS,
  STATE_SCHEMA_BASE,
  STATE_CLASSIFICATIONS,
  STATE_DEFAULT_CLASSIFICATION,
  AGGREGATE_VERSION_MAX,
  TOMBSTONE_DATASCHEMA,
} from "./state.js";
export type {
  // Solo como TIPO: su constructor recibe una conexión de NATS, y la API pública no expone ninguno.
  StateClient,
  StateEntry,
  StateEventInput,
  StateTombstoneInput,
  StateDlqInfo,
  StateDlqBuildInput,
  ParsedStateContract,
  StatePutOptions,
  StateDeleteOptions,
  StateGetOptions,
  StateWatchOptions,
  StateWriteResult,
  StateHandler,
  StateHandlerContext,
  StateSubscription,
} from "./state.js";

export {
  InvalidStateContractError,
  InvalidTenantForStateError,
  InvalidStateKeyError,
  MissingAggregateVersionError,
  StateSubjectMismatchError,
  StateConflictError,
  ReservedDomainError,
  InvalidDataClassificationError,
  RestrictedStateEntryError,
} from "./state-errors.js";

// ─── Petición/respuesta — la capacidad `rpc` (specification/11-request-reply.md) ─────────────────
// `bus.request` y `bus.respond`; los seis códigos de 04-errors.md §7; y las funciones PURAS y deterministas
// que construyen los bytes (el arnés cruzado las usa: sin reloj ni UUID).

export {
  // funciones puras
  parseRpcOperation,
  rpcSubject,
  rpcRequestType,
  rpcReplyType,
  rpcDataschema,
  resolveRpcTimeout,
  buildRpcRequest,
  buildRpcReply,
  buildRpcError,
  encodeRpcMessage,
  rpcErrorDataProblem,
  // constantes
  RPC_TIMEOUT_DEFAULT_MS,
  RPC_TIMEOUT_MIN_MS,
  RPC_TIMEOUT_MAX_MS,
  RPC_STALE_WINDOW_SECONDS,
  RPC_MAX_CONCURRENT_HANDLERS,
  RPC_STOP_TIMEOUT_MS,
  RPC_ERROR_TYPE,
  RPC_ERROR_DATASCHEMA,
  RPC_SCHEMA_BASE,
  RPC_OPERATION_PATTERN,
  RPC_ERROR_CODE_PATTERN,
  RpcSchemaError,
} from "./rpc.js";
export type {
  // Solo como TIPO: su constructor recibe una conexión de NATS, y la API pública no expone ninguno.
  RpcClient,
  ParsedRpcOperation,
  RequestOptions,
  RespondOptions,
  Responder,
  RpcHandler,
  RpcHandlerContext,
  RpcRequestInput,
  RpcRequestRef,
  RpcReplyInput,
  RpcErrorInput,
  RpcErrorData,
} from "./rpc.js";

export {
  RpcUnavailableError,
  RpcTimeoutError,
  StaleRequestError,
  RpcInvalidReplyError,
  RpcMessageTooLargeError,
  InvalidRpcCallError,
  RpcRetryableError,
  RpcPermanentError,
} from "./rpc-errors.js";
