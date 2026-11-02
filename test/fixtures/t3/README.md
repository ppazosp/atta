# T3 fixtures

Raw responses from T3 Code's MCP endpoint, recorded on 2 November 2026 against T3 Code `0.1.7-nightly.20261102.4066` with a real Codex worker on a small test repository. Home paths are replaced with `/home/dev`. Each file holds the tool name, the arguments sent, whether T3 flagged an error, and the response data.

## run

One worker, from launch to a follow-up, then a second worker that is stopped:

| Files                             | What happened                                                                                                                                                 |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `02-launch`, `03-launch-repeated` | The same launch sent twice with the same `clientRequestId`. T3 returns the same thread both times, so a launch is safe to repeat                              |
| `04` to `06`                      | Activity read in pages of 4 while the worker ran, keeping `nextPosition`                                                                                      |
| `07-steer`                        | A steer sent while the worker ran `sleep 25`                                                                                                                  |
| `08-wait`                         | The run ends with status `completed`                                                                                                                          |
| `10-activity-last-page`           | Reading past the end returns no items, `nextPosition: null` and `hasMore: false`. Keep your own position                                                      |
| `13` to `15`                      | A queued follow-up starts run ordinal 2 on the same thread                                                                                                    |
| `16` to `19`                      | A second worker stopped with `t3_thread_interrupt` and `stop: true`. The interrupt returns `interrupt_requested`; the run then ends with status `interrupted` |

## restart

A worker running `sleep 90` when the T3 server was stopped and started again:

- the run atta started (ordinal 1) ends with status `cancelled`
- T3 then starts a continuation run by itself (ordinal 2). It ends `completed`, with the assistant message "Interrupted before I could confirm completion."

So the T3 adapter reports the run atta started as interrupted, and does not treat T3's continuation run as the worker's result.
