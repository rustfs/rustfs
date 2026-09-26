# AMQP notification record layout

New AMQP bucket notifications place the S3 event directly in `Records`, matching
Kafka and webhook notification targets. For example, a consumer reads the bucket
from `Records[0].s3.bucket.name` and the object key from
`Records[0].s3.object.key`. Previously these fields were nested under
`Records[0].data` in an internal target envelope.

This follows the record layout described in the
[AWS S3 notification message structure](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html).
It changes the record envelope only; the event's existing fields and values are
preserved. The outer `EventName` and `Key` fields are retained. The outer `Key`
decodes the object name once, while `Records[0].s3.object.key` retains the event's
encoded key. AMQP audit log payloads retain their existing envelope.

## Upgrading consumers

Consumers can read event fields at the standard S3 record paths. Consumers built
around the previous AMQP envelope must be updated before upgrading producers. During rolling upgrades or queue draining, consumers
may receive both layouts. A temporary compatibility path can unwrap `record.data`
when present and otherwise use `record` directly.

Existing disk-queued messages are replayed byte for byte. Upgrading does not
rewrite their bodies. Newly queued notifications use the same flat record as
direct publishing. Retain dual-layout handling until old producers are upgraded
and old queued messages have drained. If rolling back a producer, retain that
handling while any flat messages remain queued or in the broker.

## Verification

The regression `notification_records_contain_the_event_directly` fails against
the old serializer because `Records[0]` contains the target envelope. It passes
with the notification serializer using the shared event-record builder. Unit
tests also cover encoded keys, preserved metadata in the queue, and the unchanged
audit envelope:

```bash
cargo test -p rustfs-targets --lib target::amqp::tests
```

Broker integration tests exercise direct publishing, disk-queue replay, reconnect,
and byte-for-byte replay of a pre-upgrade envelope. They use synthetic events and
require an isolated RabbitMQ-compatible broker:

```bash
RUSTFS_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5672/%2f' \
  cargo test -p rustfs-targets --test amqp_integration -- --ignored
```

These checks cover the target's serialization and delivery boundary. They do not
exercise the full bucket-notification pipeline or every downstream S3 client.
