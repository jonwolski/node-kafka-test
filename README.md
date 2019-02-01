This repository has three commits:

- the first uses `node-rdkafka` to receive messages `.on('data', ...)`. The messages are logged in the correct order.
- the second uses `node-rdkafka` as well, but the call back to `.on('data', ...)` is `async`. The messages are logged in reverse order.
- the third uses `kafkajs` which is more `async`-friendly. The message handler uses `async` / `await`, but the messages are still logged in the correct order.
