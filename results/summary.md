# Results summary

| Scenario | Transport | Orleans | DataMaxAgeInCache (s) | MetadataMinTimeInCache (s) | OnErrorAsync | Delivered | Lost | Max cached messages | Max cache bytes | Expected | Result |
|---|---|---|---:|---:|---|---|---|---:|---:|---|---|
| A | eventhub | 10.2.1 | 3 | 5 | 1 (QueueCacheMissException) | 2/2 | 0 | 9 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | eventhub | 10.3.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | memory | 10.2.1 | 3 | 5 | 1 (QueueCacheMissException) | 2/2 | 0 | 9 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | memory | 10.3.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| B | eventhub | 10.2.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | eventhub | 10.3.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | memory | 10.2.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | memory | 10.3.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| C | eventhub | 10.2.1 | 3 | 5 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | 28 | 1048576 | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | eventhub | 10.3.1 | 3 | 5 | 1 (QueueCacheMissException) | 2/21 | 19 (2..20) | 28 | 1048576 | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | memory | 10.2.1 | 3 | 5 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | 27 | 1048576 | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | memory | 10.3.1 | 3 | 5 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | 28 | 1048576 | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| D | eventhub | 10.2.1 | 3 | 600 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | eventhub | 10.3.1 | 3 | 600 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | memory | 10.2.1 | 3 | 600 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | memory | 10.3.1 | 3 | 600 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E | eventhub | 10.2.1 | 3 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 9 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E | eventhub | 10.3.1 | 3 | 5 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (idle cursor refreshed to event 2), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E | memory | 10.2.1 | 3 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 9 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E | memory | 10.3.1 | 3 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 9 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| A-mid | eventhub | 10.2.1 | 6 | 5 | 1 (QueueCacheMissException) | 2/2 | 0 | 15 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-mid | eventhub | 10.3.1 | 6 | 5 | 0 | 2/2 | 0 | 15 | 1048576 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-mid | memory | 10.2.1 | 6 | 5 | 1 (QueueCacheMissException) | 2/2 | 0 | 15 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-mid | memory | 10.3.1 | 6 | 5 | 0 | 2/2 | 0 | 15 | 1048576 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-mid | eventhub | 10.2.1 | 6 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 15 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E-mid | eventhub | 10.3.1 | 6 | 5 | 0 | 2/2 | 0 | 15 | 1048576 | no OnErrorAsync (idle cursor refreshed to event 2), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-mid | memory | 10.2.1 | 6 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 14 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E-mid | memory | 10.3.1 | 6 | 5 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 15 | 1048576 | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| A-big | eventhub | 10.2.1 | 40 | 5 | 0 | 2/2 | 0 | 60 | 1048576 | no OnErrorAsync (expectedToken still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-big | eventhub | 10.3.1 | 40 | 5 | 0 | 2/2 | 0 | 60 | 1048576 | no OnErrorAsync (expectedToken still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-big | memory | 10.2.1 | 40 | 5 | 0 | 2/2 | 0 | 62 | 1048576 | no OnErrorAsync (expectedToken still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-big | memory | 10.3.1 | 40 | 5 | 0 | 2/2 | 0 | 61 | 1048576 | no OnErrorAsync (expectedToken still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-big | eventhub | 10.2.1 | 40 | 5 | 0 | 2/2 | 0 | 32 | 1048576 | no OnErrorAsync (idle cursor position still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-big | eventhub | 10.3.1 | 40 | 5 | 0 | 2/2 | 0 | 32 | 1048576 | no OnErrorAsync (idle cursor position still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-big | memory | 10.2.1 | 40 | 5 | 0 | 2/2 | 0 | 33 | 1048576 | no OnErrorAsync (idle cursor position still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-big | memory | 10.3.1 | 40 | 5 | 0 | 2/2 | 0 | 33 | 1048576 | no OnErrorAsync (idle cursor position still cached), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-meta | eventhub | 10.2.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-meta | eventhub | 10.3.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-meta | memory | 10.2.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A-meta | memory | 10.3.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-meta | eventhub | 10.2.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-meta | eventhub | 10.3.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-meta | memory | 10.2.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E-meta | memory | 10.3.1 | 3 | 40 | 0 | 2/2 | 0 | 9 | 1048576 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| C-big | eventhub | 10.2.1 | 40 | 5 | 0 | 21/21 | 0 | 47 | 1048576 | no OnErrorAsync, all events delivered (lag shorter than DataMaxAgeInCache) | PASS |
| C-big | eventhub | 10.3.1 | 40 | 5 | 0 | 21/21 | 0 | 47 | 1048576 | no OnErrorAsync, all events delivered (lag shorter than DataMaxAgeInCache) | PASS |
| C-big | memory | 10.2.1 | 40 | 5 | 0 | 21/21 | 0 | 48 | 1048576 | no OnErrorAsync, all events delivered (lag shorter than DataMaxAgeInCache) | PASS |
| C-big | memory | 10.3.1 | 40 | 5 | 0 | 21/21 | 0 | 48 | 1048576 | no OnErrorAsync, all events delivered (lag shorter than DataMaxAgeInCache) | PASS |
