# microsoft-azure-function-sql

# SQL Database

## Drop all tables

```SQL
DROP TABLE touch_events;
DROP TABLE touch_events_latest;
DROP TABLE prox_events;
DROP TABLE prox_events_latest;
DROP TABLE temperature_events;
DROP TABLE temperature_events_latest;
```

## Setup new tables

```SQL
CREATE TABLE temperature_events (
  event_id char(20) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  temperature float NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);

CREATE TABLE temperature_events_latest (
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  temperature float NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);

CREATE TABLE prox_events (
  event_id char(20) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  state varchar(15) NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);

CREATE TABLE prox_events_latest (
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  state varchar(15) NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);

CREATE TABLE touch_events (
  event_id char(20) NOT NULL UNIQUE,
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);

CREATE TABLE touch_events_latest (
  device_id char(20) NOT NULL,
  timestamp datetime NOT NULL,
  name char(64),
  area char(64),
  area2 char(64),
);
```