#!/bin/bash
set -a
source deploy/embedded/.env.owner.local
set +a
cd desktop
node --input-type=module -e \
  'import { nsecEncode } from "nostr-tools/nip19";
  console.log(nsecEncode(Buffer.from(process.env.BUZZ_PRIVATE_KEY, "hex")))'
