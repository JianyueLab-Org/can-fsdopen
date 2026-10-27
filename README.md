# can-fsdopen

A standalone FSD (Flight Sim Daemon) server written in Go. It connects flight simulator and air traffic control clients, distributes traffic, and manages coordination.

The project contains the protocol core and local authentication. It does not depend on CAN membership services, a database, or a website.

> Plaintext FSD connections transmit login passwords without transport encryption. The default listener is loopback-only. For public deployments, use TLS-capable clients and a TLS listener. Do not reuse passwords from other networks or websites.

## Features

- **FSD protocol:** revisions 9, 100, and 101, including client greetings, login, and packet framing.
- **Traffic distribution:** pilot position broadcasts, controller visibility ranges, and Velocity fast positions.
- **Communication:** text messages, ATIS, and in-network supervisor messages.
- **Flight plans:** in-memory filing, queries, controller amendments, and short-term reconnect recovery.
- **Radar coordination:** track ownership, handoff requests, acceptance, explicit ownership transfer, release, and disconnect cleanup.
- **Training mode:** additional Sweatbox listeners.
- **Connection protection:** login deadlines, per-IP admission limits, authentication throttling, packet and byte rate limits, and wallop budgets.
- **Optional capabilities:** FSD-over-TLS, operator-configured METAR sources, and client challenge/response keys.

There is no member registration, division roster, database, session-duration storage, or persistent position history.
The server does not provide HTTP datafeeds, SSE, website APIs, CRC automation, Discord forwarding, external data reporting, or raw packet capture.

## Quick start

Run the following commands from the project root in a Linux or macOS shell.
Go **1.27.3 or newer** is required. No database is needed.

### 1. Build and copy the configuration

```sh
go build -trimpath -o bin/fsdopen ./cmd/fsdopen
cp config.example.json config.json
```

Do not overwrite `config.json` if you already have a local configuration.
The program reads existing files; it does not create accounts or configuration files automatically.

### 2. Generate a password hash

```sh
./bin/fsdopen hash-password
```

Enter a password at the prompt. Terminal input is not echoed, and the command outputs only the bcrypt hash.
Use that output as `password_hash` in the next step. Do not store the plaintext password in the account file.

Passwords must be **1–72 bytes** and cannot contain colons, line breaks, or NUL.
Both generated and loaded hashes use **bcrypt cost 12**.

### 3. Create a local account

Create `users.json`, replacing the instructional text below with the complete hash generated in the previous step:

```json
{
  "users": [
    {
      "id": "900001",
      "password_hash": "Replace with the complete bcrypt hash from hash-password",
      "rating": 5
    }
  ]
}
```

Account file rules:

- `id` is a **string of 1–12 digits** assigned by the server operator. It does not need to be a real member ID from another network.
- Each `id` must be unique. The file must contain at least one account.
- `rating` is an integer from -1 to 12. Accounts with a rating of -1 or 0 cannot log in.
- A client's requested login rating cannot exceed the account's authorized rating.
- The file does not accept plaintext passwords, names, email addresses, or other undefined fields.

Common ratings are 1 OBS, 2 S1, 3 S2, 4 S3, 5 C1, 11 SUP, and 12 ADM.
Strict facility checks are enabled by default: DEL/GND/ATIS require at least S1, TWR requires S2, APP requires S3, and CTR/FSS require C1.
Ratings 11 and 12 grant supervisor privileges and should be assigned only to trusted operators.

There are no default accounts and no authentication bypass mode.

### 4. Start the server

```sh
chmod 600 users.json
./bin/fsdopen -config config.json
```

The default FSD address is **`127.0.0.1:6809`**.
Missing credential files, malformed accounts, or invalid configuration cause the program to exit before opening a listener.

### 5. Connect a client

In a client that supports custom FSD servers, configure:

- Server address: `127.0.0.1`.
- Port: `6809`.
- CID/user ID: the account's `id` from `users.json`.
- Password: the plaintext password used to generate the account's hash, not the hash itself.
- Login rating: no higher than the account's authorized rating.
- Callsign: an aircraft callsign for pilots or the appropriate position callsign for controllers.

EuroScope can use protocol revision 9 for private, non-VATSIM server logins.
This project does not include third-party client challenge/response keys. Operators must supply keys they are authorized to use when a client requires that mechanism.
Support for a protocol revision does not imply support for every client's proprietary extensions.

## Configuration

Start with [`config.example.json`](config.example.json):

```json
{
  "users_file": "users.json",
  "listen_addrs": ["127.0.0.1:6809"],
  "sweatbox_listen_addrs": ["127.0.0.1:6820"],
  "fsd_mode": 0,
  "facility_enforcement": "strict",
  "weather": "",
  "weather_fallback": "",
  "client_auth_keys": {}
}
```

Omitted fields use defaults. Unknown fields, incorrect types, and `null` values are rejected.
Configuration is read from JSON; `.env` files are not loaded or applied as overrides.
Relative paths in `users_file`, `tls_cert_file`, and `tls_key_file` are resolved against the directory containing the configuration file.

### Listeners and protocol

- `listen_addrs`: plaintext FSD listeners. The default is loopback port `6809`.
- `fsd_mode`: `0` selects normal mode; `1` adds the training listeners in `sweatbox_listen_addrs`, which default to loopback port `6820`. Both ports belong to the same server instance.
- `welcome`: the message sent after login. Colons and multiline text using LF or CRLF are supported. Each line is sent as a separate server text message. Bare CR and other unsafe control characters are rejected.
- `ident_delay_ms`: defaults to 1500 milliseconds for compatibility with clients that initiate login before receiving a greeting.
- `server_ident` / `server_ident_sweatbox`: protocol version strings recognized by clients, not network names. Keep the defaults to preserve existing greeting compatibility.
- `client_auth_keys`: an optional mapping from client software IDs to challenge/response keys. Empty by default. Do not commit keys you are not authorized to redistribute.

### Ratings and connection limits

`facility_enforcement` accepts `strict`, `warn`, or `off`, and defaults to `strict`.
It controls generic facility rating checks, not region, division, or membership rosters.
Disabling facility checks does not bypass account suspension, login rating ceilings, or OBS account restrictions.

Connection protection settings include:

- `max_clients`, `max_pre_auth_connections`, and `max_pre_auth_per_ip`.
- `auth_concurrency` and `login_timeout_seconds`.
- `auth_attempts_per_minute`, `auth_attempt_burst`, and `auth_ip_state_limit`.
- `inbound_packets_per_second` and `inbound_bytes_per_second`.
- `wallops_per_client_minute` and `wallops_per_server_minute`.

See [`config/config.go`](config/config.go) for all fields, defaults, and validation ranges.

### METAR

`weather` and `weather_fallback` are empty by default, so no external weather requests are made.
When configured, the sources are tried in order. The requested ICAO station code is appended directly to each URL prefix.
Sources must use HTTP(S) and return text containing a METAR line that starts with the station code.

Weather requests send the station code, not account credentials, flight plans, or traffic state.

## TLS deployment

Provide a PEM certificate and private key that your clients trust, then use a configuration such as:

```json
{
  "users_file": "users.json",
  "plaintext_policy": "off",
  "tls_listen_addrs": ["0.0.0.0:6808"],
  "tls_cert_file": "certs/server.pem",
  "tls_key_file": "certs/server.key"
}
```

This configuration exposes only a TLS listener. Clients must explicitly connect to the TLS port.
Clients without TLS support or trust in the certificate cannot use this endpoint.
When `plaintext_policy` is `off`, at least one TLS listener must be enabled for the active mode.

Training mode can add listeners through `sweatbox_tls_listen_addrs`.
Failure to load the certificate or private key stops startup; the server does not fall back to plaintext.

## Operation and reloads

Command-line options:

```sh
./bin/fsdopen -config config.json
./bin/fsdopen -config config.json -debug
```

Logs are written to standard error. `-debug` enables debug event logging.
Send `SIGINT` or `SIGTERM` to stop listening and close connections.

`SIGHUP` reloads the configuration file:

- Welcome text, weather sources, facility enforcement, optional client keys, and admission limits can be reloaded.
- Login deadlines and greeting delays apply to subsequent connections. Per-connection packet, byte, and wallop limits are initialized for subsequent sessions.
- Listener addresses, TLS settings, account file paths, mode, protocol identifiers/seeds, plaintext policy, maximum online clients, and the server-wide wallop budget require a restart.
- A reload containing startup-only changes or invalid values is rejected as a whole. The previous configuration remains active.

Changes to the contents of `users.json` also require a restart. `SIGHUP` does not reload accounts.

## Data and privacy boundaries

At runtime, FSD processes client-supplied CIDs, names, IP addresses, positions, flight plans, and messages.
Names and identity fields may be sent to other clients through the protocol. Supervisor queries have their corresponding access privileges.
This project does not guarantee identity anonymization.

Traffic, flight plans, and track ownership are held in memory and cleared on restart.
Disconnected flight plans have a reconnect window of up to five minutes, subject to capacity limits. Recovery requires the same identity and callsign.

Ordinary logs contain callsigns, events, and necessary operational information. They do not record CIDs, peer IP addresses, passwords, password hashes, or message bodies.
Operator-configured listener addresses, certificate paths, and system errors may appear in diagnostics.

Treat account hashes, client keys, certificate private keys, and deployment configuration as sensitive files.
The default `config.json`, `users.json`, PEM files, and private key files are ignored by Git. Add ignore rules for custom filenames yourself.

## Docker

Build from the project root:

```sh
docker build -t can-fsdopen .
```

Before starting the container, change `listen_addrs` in your local `config.json` to `["0.0.0.0:6809"]`.
The following example publishes the container port only on the host's loopback address for local verification:

```sh
docker run --rm --name can-fsdopen \
  --user "$(id -u):$(id -g)" \
  -p 127.0.0.1:6809:6809 \
  -v "$PWD/config.json:/config/config.json:ro" \
  -v "$PWD/users.json:/config/users.json:ro" \
  can-fsdopen
```

The process runs with the account file owner's UID/GID, allowing `users.json` to retain mode 0600.
The image contains the executable, `LICENSE`, `THIRD_PARTY_NOTICES.md`, `SOURCE.txt`, and a `source.tar.gz` archive of its Corresponding Source.
It does not include local accounts, deployment configuration, certificates, or private keys.
TLS deployments require additional read-only certificate/key mounts and publication of the TLS port.
The image does not provide an HTTP health-check endpoint.

## Release archives and Corresponding Source

Build a release with the same packaging script used by the Dockerfile:

```sh
./tools/release.sh
```

For a static Linux/amd64 release:

```sh
GOOS=linux GOARCH=amd64 ./tools/release.sh
```

The default output is `dist/can-fsdopen-<GOOS>-<GOARCH>/` and a matching `.tar.gz` archive.
Pass a new output directory as the first argument to select another destination.
Existing release output must not be overwritten; use a new destination for a repeated build.

Each release includes the executable, the AGPL license, complete third-party license notices, source-access instructions, and `source.tar.gz`.
The source archive contains the actual build snapshot, build scripts, and vendored Go dependencies.
It excludes local account/configuration files, private keys, Git metadata, build outputs, and scratch files.
The Go compiler and standard library are supplied by the supported Go toolchain.

To rebuild without fetching module dependencies, extract `source.tar.gz` into an empty directory, enter `can-fsdopen-source/`, and run:

```sh
go build -mod=vendor -trimpath -buildvcs=false -o fsdopen ./cmd/fsdopen
```

For container distributions, the same source archive is at `/app/source.tar.gz`, with instructions in `/app/SOURCE.txt`.
Keep the source archive and license files with every distributed binary or image; publishing an executable alone is not the release format.

## Development and embedding

```sh
go vet ./...
go test ./...
go test -race ./...
```

Tests use synthetic identities, local connections, and controlled weather responses. They do not require a database or a live membership API.

Code entry points:

- [`cmd/fsdopen`](cmd/fsdopen): standalone command, signal handling, and password hashing utility.
- [`fsd`](fsd): protocol, connections, traffic distribution, in-memory flight plans, and track state machine.
- [`auth`](auth): local bcrypt authentication and failed-attempt throttling.
- [`config`](config): strict JSON configuration, defaults, and reloads.

To embed the server in another Go program, supply configuration and an implementation of `fsd.Authenticator` through `fsd.Options`.
The core does not require a local account file or any membership database tables.

## License and attribution

Copyright (c) 2026 Cerulean Aviation Network contributors.

This project is licensed under the [GNU Affero General Public License, version 3 only](LICENSE) (`AGPL-3.0-only`).
If you modify the program and allow users to interact with it remotely over a network, section 13 requires you to prominently offer those users access to the Corresponding Source of your modified version.

The protocol core was extracted from can-fsd. Upstream openfsd material retains its MIT license.
See [Third-party notices](THIRD_PARTY_NOTICES.md) for the complete upstream MIT notice and Go dependency attribution.
This is not an official VATSIM server and does not grant authorization to connect to the VATSIM network.
