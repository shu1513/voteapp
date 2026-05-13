import { runStateResourcesValidator } from './src/pipeline/validators/stateResourcesValidator.ts';

runStateResourcesValidator({ once: true, batchSize: 200, blockMs: 1000 })
  .then(() => console.log('validator_done'))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
