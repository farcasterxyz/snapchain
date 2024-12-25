import { NodeError } from '../errors';

function processMessage(msg: Message) {
  try {
    // Previous code...
  } catch (error) {
    throw new NodeError(`Failed to process message: ${error.message}`);
  }
} 