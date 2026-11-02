# Connect to ChesapeakeDev Buzz

ChesapeakeDev Buzz is a private community. Its community address is:

```text
wss://buzz.chesapeake.dev
```

New members need an invite link from a ChesapeakeDev owner or administrator.
If you already joined this community on the identity in your app, you can use
the community address above instead.

## What you need

- Buzz Desktop on a computer
- An invite link from a ChesapeakeDev owner or administrator
- Buzz Mobile on your phone, if you want to use Buzz from your phone

ChesapeakeDev does not currently publish its own signed desktop or mobile
packages. You can use a compatible upstream Buzz client or a build supplied by
your administrator. Only install software from a source you trust.

## Connect Buzz Desktop

1. Open Buzz Desktop and complete the identity setup. Back up the recovery
   information it gives you and keep it private.
2. Select **Join a community**.
3. Paste your ChesapeakeDev invite link. If this identity is already a member,
   enter `wss://buzz.chesapeake.dev` instead.
4. Confirm that the app shows the ChesapeakeDev community.

An invite link is a membership credential. Send it only to its intended
recipient, and do not post it publicly. Never send anyone your private key,
recovery secret, or an `nsec` value.

## Pair your phone

Pairing securely copies your existing Buzz identity and community connection
from Desktop to Mobile. Do not create a second identity on the phone.

If Desktop reports that mobile pairing is unavailable, continue using Desktop
and contact a ChesapeakeDev administrator. The community and its separate
pairing service must both be online for these steps to work.

1. On Desktop, open **Settings**, select **Mobile**, and choose
   **Start pairing**.
2. Open Buzz Mobile on your phone and scan the QR code shown by Desktop. If you
   cannot scan it, choose **Use pairing code** on the phone and paste the code
   from Desktop.
3. Compare the six-digit verification code on both devices.
4. Continue only if both codes match exactly, then confirm the match on both
   devices.
5. Wait for Desktop to show **Paired** and for Mobile to open the ChesapeakeDev
   community.

The QR code and pairing code are temporary credentials. Do not share them or
scan a code sent by someone else. Pair while you control both devices.

## Troubleshooting

- **The community rejects the connection:** Ask an owner or administrator for
  a fresh invite link. Entering the community address alone does not enroll a
  new identity in this private community.
- **The invite link opens a browser instead of Buzz:** Copy the complete link,
  open Buzz Desktop, choose **Join a community**, and paste it there.
- **Pairing cannot reach the relay:** Check that both devices are online and
  start a new pairing session. The pairing service is
  `wss://pairing.buzz.chesapeake.dev`; Buzz normally discovers it
  automatically.
- **The verification codes differ:** Cancel immediately and start again. Do not
  approve mismatched codes.
- **The QR or pairing code expired:** Select **Generate new pairing code** on
  Desktop and scan or paste the new one.
- **You replaced or lost a device:** Contact a ChesapeakeDev owner or
  administrator. Do not share private recovery material while asking for help.

You can check whether the public community service is reachable by opening
<https://buzz.chesapeake.dev/> in a browser. The address entered in a Buzz
client remains `wss://buzz.chesapeake.dev`.
