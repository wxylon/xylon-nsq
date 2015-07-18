## nsqadmin

`nsqadmin` is the Web UI to view message statistics and to perform administrative tasks like removing a channel.

### Command Line Options

    -graphite-url="": URL to graphite HTTP address
    -http-address="0.0.0.0:4171": <addr>:<port> to listen on for HTTP clients
    -lookupd-http-address=[]: lookupd HTTP address (may be given multiple times)
    -notification-http-endpoint="": HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent
    -nsqd-http-address=[]: nsqd HTTP address (may be given multiple times)
    -proxy-graphite=false: Proxy HTTP requests to graphite
    -template-dir="": path to templates directory
    -use-statsd-prefixes=true: expect statsd prefixed keys in graphite (ie: 'stats_counts.')
    -version=false: print version string

### statsd / Graphite Integration

When using `nsqd --statsd-address=...` you can specify a `nsqadmin
--graphite-url=http://graphite.yourdomain.com` to enable graphite charts in `nsqadmin`. If using a
statsd clone (like [gographite](https://github.com/bitly/gographite)) that does not prefix keys,
also specify `--use-statsd-prefix=false`.

### Admin Notifications

If the `--notification-http-endpoint` flag is set, `nsqadmin` will send a POST request to the
specified (fully qualified) endpoint each time an admin action (such as pausing a channel) is
performed.

The body of the request contains information about the action, like so:

> {
>   "action": "unpause\_channel",
>   "channel": "mouth",
>   "topic": "beer",
>   "timestamp": 1357683731,
>   "user": "df",
>   "user\_agent": "Mozilla/5.0 (Macintosh; Iphone 8)"
>   "remote\_ip": "1.2.3.4:5678"
> }

The `user` field will be filled if a username is present in the request made to `nsqadmin`, say if
it were running with htpasswd authentication or behind [google-auth-proxy][gaproxy], otherwise it
will be an empty string. The `channel` field will also be an empty string when not applicable.

Hint: You can create an NSQ stream of admin action notifications with the topic name `admin_actions`
by setting `--notification-http-endpoint` to `http://addr.of.nsqd/put?topic=admin_actions`

[gaproxy]: https://github.com/bitly/google_auth_proxy
