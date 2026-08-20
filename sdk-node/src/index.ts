/**
 * flux SDK para Node.js — Event Protocol v1, nivel de conformidad L2.
 *
 * Especificación: https://github.com/charlessonamericantrading/flux
 */

export { connect, FluxBus, ConsumerConfigMismatchError } from "./client.js";
export type {
  ConnectOptions,
  PublishOptions,
  SubscribeOptions,
  HandlerContext,
  Handler,
  Subscription,
} from "./client.js";

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
} from "./protocol.js";
export type { ParsedSubject } from "./protocol.js";

export { currentContext } from "./context.js";
export type { EventContext } from "./context.js";
