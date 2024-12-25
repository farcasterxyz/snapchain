import { NodeError } from './errors';

// Previous code...
if (!isValidConfig(config)) {
  throw new NodeError('Invalid configuration');
}
// Rest of file... 