# Results summary

| Scenario | Transport | Orleans | OnErrorAsync | Delivered | Lost | Expected | Result |
|---|---|---|---|---|---|---|---|
| A | eventhub | 10.2.1 | 1 (QueueCacheMissException) | 2/2 | 0 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | eventhub | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | memory | 10.2.1 | 1 (QueueCacheMissException) | 2/2 | 0 | 1 x OnErrorAsync(QueueCacheMissException) at the handshake, then OnNextAsync(2); nothing lost; 1 activation | PASS |
| A | memory | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (miss caught, cursor at cacheToken), OnNextAsync(2); nothing lost; 1 activation | PASS |
| B | eventhub | 10.2.1 | 0 | 2/2 | 0 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | eventhub | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | memory | 10.2.1 | 0 | 2/2 | 0 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| B | memory | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (GetSequenceToken returns null), OnNextAsync(2); nothing lost; 2 activations | PASS |
| C | eventhub | 10.2.1 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | eventhub | 10.3.1 | 1 (QueueCacheMissException) | 2/21 | 19 (2..20) | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | memory | 10.2.1 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| C | memory | 10.3.1 | 2 (QueueCacheMissException, QueueCacheMissException) | 1/21 | 20 (2..21) | OnErrorAsync(QueueCacheMissException) on both versions and events skipped | PASS |
| D | eventhub | 10.2.1 | 0 | 2/2 | 0 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | eventhub | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | memory | 10.2.1 | 0 | 2/2 | 0 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| D | memory | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (cache resumes at its oldest message), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E | eventhub | 10.2.1 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E | eventhub | 10.3.1 | 0 | 2/2 | 0 | no OnErrorAsync (idle cursor refreshed to event 2), OnNextAsync(2); nothing lost; 1 activation | PASS |
| E | memory | 10.2.1 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
| E | memory | 10.3.1 | 1 (QueueCacheMissException) | 1/2 | 1 (2) | 1 x OnErrorAsync(QueueCacheMissException) from the idle cursor, event 2 skipped; 1 activation | PASS |
