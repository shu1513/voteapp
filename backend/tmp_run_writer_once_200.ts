import { runStateResourcesWriter } from './src/pipeline/writers/stateResourcesWriter.ts';

runStateResourcesWriter({ once: true, batchSize: 200, blockMs: 1000 })
  .then(() => console.log('writer_done'))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
