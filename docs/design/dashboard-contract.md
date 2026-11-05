# Dashboard connection boundary

CoreQ supplies the linked connection client under [R43](../../coreq.md#product-connections-enroll), not a dashboard service. [Enrollment](enrollment.md) defines its pairing, protected connection state, bounded delivery, accepted/projected receipts, disconnect recovery and capability-scoped operation transport.

Consumers own their payloads, queues, service scheduling and interpretation of receipts. [DashQ](https://github.com/taskqorg/dashq/blob/main/docs/design/enrollment.md) owns server routes, grants and projection. Product actions require their existing capability and authority checks; a telemetry connection grants no general command authority.

The former pull-first source-frame and action-adapter research is superseded as connection guidance. It defines no active CoreQ API or pending owner decision. Historical findings remain in [CoreQ #22](https://github.com/taskqorg/coreq/issues/22) and repository history.

[Universal telemetry](telemetry.md) remains the storage and transport contract. This documentation cleanup changes no producer, native service, connection grant or retained observation.
