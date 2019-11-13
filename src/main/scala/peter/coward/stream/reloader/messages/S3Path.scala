package peter.coward.stream.reloader.messages

case class S3Path(bucket: String, path: String, eventName: String, eventVersion: String)
