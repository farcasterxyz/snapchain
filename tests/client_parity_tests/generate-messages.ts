#!/usr/bin/env ts-node
import * as fs from 'fs';
import * as path from 'path';
import {
  makeCastAdd,
  makeUserDataAdd,
  makeReactionAdd,
  makeLinkAdd,
  CastAddBody,
  UserDataBody,
  UserDataType,
  FarcasterNetwork,
  Message,
  ReactionBody,
  ReactionType,
  LinkBody,
  NobleEd25519Signer,
  CastType
} from '@farcaster/core';

function getPrivateKey() {
  // Use a fixed private key for deterministic generation
  const privateKeyHex = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  return new Uint8Array(Buffer.from(privateKeyHex, 'hex'));
}

function getFixedFarcasterTime(): number {
  // Use a fixed timestamp for deterministic generation
  const FARCASTER_EPOCH = 1609459200;
  const FIXED_TIMESTAMP = 1704067200; // January 1, 2024 UTC
  return FIXED_TIMESTAMP - FARCASTER_EPOCH;
}

async function createValidCastAddMessage(): Promise<Message> {
  const privateKey = getPrivateKey();

  const castAddBody: CastAddBody = {
    text: 'Hello, Farcaster!',
    mentions: [],
    mentionsPositions: [],
    embeds: [],
    embedsDeprecated: [],
    parentCastId: undefined,
    parentUrl: undefined,
    type: CastType.CAST
  };

  const dataOptions = {
    fid: 12345,
    network: FarcasterNetwork.TESTNET,
    timestamp: getFixedFarcasterTime()
  };

  const signer = new NobleEd25519Signer(privateKey);

  const result = await makeCastAdd(castAddBody, dataOptions, signer);
  return result._unsafeUnwrap();
}

async function createValidUserDataAddMessage(): Promise<Message> {
  const privateKey = getPrivateKey();

  const userDataBody: UserDataBody = {
    type: UserDataType.DISPLAY,
    value: 'Test User'
  };

  const dataOptions = {
    fid: 67890,
    network: FarcasterNetwork.TESTNET,
    timestamp: getFixedFarcasterTime()
  };

  const signer = new NobleEd25519Signer(privateKey);

  const result = await makeUserDataAdd(userDataBody, dataOptions, signer);
  return result._unsafeUnwrap();
}

async function createValidReactionAddMessage(): Promise<Message> {
  const privateKey = getPrivateKey();

  const reactionBody: ReactionBody = {
    type: ReactionType.LIKE,
    targetCastId: {
      fid: 123,
      hash: new Uint8Array([...Array(20)].map((_, i) => i + 1))
    }
  };

  const dataOptions = {
    fid: 11111,
    network: FarcasterNetwork.TESTNET,
    timestamp: getFixedFarcasterTime()
  };

  const signer = new NobleEd25519Signer(privateKey);

  const result = await makeReactionAdd(reactionBody, dataOptions, signer);
  return result._unsafeUnwrap();
}

async function createValidLinkAddMessage(): Promise<Message> {
  const privateKey = getPrivateKey();

  const linkBody: LinkBody = {
    type: 'follow',
    targetFid: 456,
    displayTimestamp: undefined
  };

  const dataOptions = {
    fid: 22222,
    network: FarcasterNetwork.TESTNET,
    timestamp: getFixedFarcasterTime()
  };

  const signer = new NobleEd25519Signer(privateKey);

  const result = await makeLinkAdd(linkBody, dataOptions, signer);
  return result._unsafeUnwrap();
}

async function createInvalidSignatureMessage(): Promise<Message> {
  const validMessage = await createValidCastAddMessage();

  // Corrupt the signature
  const corruptedSignature = new Uint8Array(64);
  corruptedSignature.fill(0);

  return {
    ...validMessage,
    signature: corruptedSignature
  };
}

async function createInvalidHashMessage(): Promise<Message> {
  const validMessage = await createValidCastAddMessage();

  // Corrupt the hash
  const corruptedHash = new Uint8Array(20);
  corruptedHash.fill(0);

  return {
    ...validMessage,
    hash: corruptedHash
  };
}

async function generateTestMessages() {
  const outputDir = 'input_messages';

  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(path.join(__dirname, outputDir), { recursive: true });
  }

  const testInputs =
    [
      {
        filename: 'valid_cast_add.bin',
        message: await createValidCastAddMessage(),
        isValid: true
      },
      {
        filename: 'valid_user_data_add.bin',
        message: await createValidUserDataAddMessage(),
        isValid: true
      },
      {
        filename: 'valid_reaction_add.bin',
        message: await createValidReactionAddMessage(),
        isValid: true
      },
      {
        filename: 'valid_link_add.bin',
        message: await createValidLinkAddMessage(),
        isValid: true
      },
      {
        filename: 'invalid_signature.bin',
        message: await createInvalidSignatureMessage(),
        isValid: false
      },
      {
        filename: 'invalid_hash.bin',
        message: await createInvalidHashMessage(),
        isValid: false
      },
    ]

  for (const testInput of testInputs) {
    fs.writeFileSync(
      path.join(__dirname, outputDir, testInput.filename),
      Message.encode(testInput.message).finish()
    );
  }

  console.log('All test message files generated successfully!');

  const testInputsJson = testInputs.map((testInput) => {
    return {
      message_file: `${outputDir}/${testInput.filename}`,
      is_valid: testInput.isValid
    }
  });

  fs.writeFileSync(
    path.join(__dirname, 'test_inputs.json'),
    JSON.stringify(testInputsJson, null, 2)
  );

  console.log('Updated test_inputs.json');
}

if (require.main === module) {
  generateTestMessages().catch(console.error);
}

export { generateTestMessages };