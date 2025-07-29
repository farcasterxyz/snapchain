import * as fs from 'fs';
import * as path from 'path';
import { Message, validations } from '@farcaster/core';

interface TestInput {
  message_file: string;
  is_valid: boolean;
}

interface TestCase {
  message: Message;
  is_valid: boolean;
  filename: string;
}

function loadTestData(): TestCase[] {
  const testDataPath = path.join(__dirname, 'test_inputs.json');
  const baseDir = __dirname;

  const data = fs.readFileSync(testDataPath, 'utf8');
  const testInputs: TestInput[] = JSON.parse(data);

  return testInputs.map(input => {
    const filePath = path.join(baseDir, input.message_file);
    const messageBytes = fs.readFileSync(filePath);
    const message = Message.decode(new Uint8Array(messageBytes));

    return {
      message,
      is_valid: input.is_valid,
      filename: input.message_file,
    };
  });
}

describe('validations parity', () => {
  test('basic', async () => {
    const testCases = loadTestData();
    for (const testCase of testCases) {
      const validationResult = await validations.validateMessage(testCase.message)
      const isValid = validationResult.isOk();
      expect(isValid).toBe(testCase.is_valid);
    }
  });
});