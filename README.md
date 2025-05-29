Weak Entropy in BitcoinJS-lib v0.1.3: An Analysis

Between 2010 and 2015, numerous Bitcoin exchanges and web wallets utilized BitcoinJS-lib v0.1.3 to generate wallets. However, many browsers at the time failed to implement window.crypto.getRandomValues, falling back instead to the insecure Math.random() for entropy. This significantly compromised the randomness of generated private keys.

Entropy Generation via Math.random()

The following JavaScript snippet illustrates how randomness was seeded:

if (this.pool == null) {
  this.poolSize = 256;
  this.pool = new Array(this.poolSize);
  this.pptr = 0;
  let t;

  while (this.pptr < this.poolSize) {
    t = Math.floor(65536 * Math.random());
    this.pool[this.pptr++] = t >>> 8;
    this.pool[this.pptr++] = t & 255;
  }

  this.pptr = 0;
  this.seedTime();
}

This code initializes a pool with 256 values using random 16-bit integers derived from Math.random(). Each value is split into a high and low byte.

Example:

t = Math.floor(65536 * 0.5533) // ~36222
this.pool[this.pptr++] = t >>> 8; // 141
this.pool[this.pptr++] = t & 255; // 222

Resulting in a byte pool similar to:

[141, 222, 70, 233, 237, 155, ...]

XOR Seeding with Time

To introduce additional entropy, a timestamp is XORed into the pool:

this.seedInt = function (x) {
  this.pool[this.pptr++] ^= x & 255;
  this.pool[this.pptr++] ^= (x >> 8) & 255;
  this.pool[this.pptr++] ^= (x >> 16) & 255;
  this.pool[this.pptr++] ^= (x >> 24) & 255;
  if (this.pptr >= this.poolSize) this.pptr -= this.poolSize;
};

Despite the XOR operation, the reliance on a predictable seed time and Math.random() renders the resulting pool easily reproducible.

RC4 (Arcfour) Cipher Application

After seeding, the pool is processed using an RC4 stream cipher (referred to as Arcfour):

this.getByte = function () {
  if (this.state == null) {
    this.seedTime();
    this.state = this.ArcFour();
    this.state.init(this.pool);
    for (this.pptr = 0; this.pptr < this.pool.length; ++this.pptr)
      this.pool[this.pptr] = 0;
    this.pptr = 0;
  }
  return this.state.next();
};

Once seeded, this RC4 cipher generates pseudo-random output based on the original insecure pool.

Predicting Math.random()

Because Math.random() is deterministic in V8-based browsers, it is feasible to predict its output using tools such as:

v8-randomness-predictor

v8_rand_buster

This enables reverse-engineering of the entire entropy generation process for wallets created using weak RNG.

Real-World Exploit: Coinpunk

Projects such as Coinpunk implemented this vulnerable entropy mechanism. By knowing the seed time (e.g., 1294200190000), attackers could recreate the exact private keys used during wallet generation.



Identifying Vulnerable Wallets

Although the vulnerability only affects wallets generated using BitcoinJS-lib v0.1.3, it's often unclear which library created a given address. A helpful approach is to determine the timestamp of the first transaction:

Address: 1NUhcfvRthmvrHf1PAJKe5uEzBGK44ASBD
First Transaction: 2014-03-16 23:48:51 GMT-7

Convert the date to Unix time:

date -d "2014-03-16 23:48:51 GMT-7" +"%s" | awk '{print $1 * 1000}'
# Output: 1395038931000

Seed Scanning

You can write a script to simulate the RNG by seeding it from a known timestamp and iterating forward:

Seed: 1310691661000
Hex: 6ad2d763712eae6428e2922d7181f92fb70d0e564d1dd38dd0aa9b34b844c0cb
P2PKH: 1JbryLqejpB17zLDNspRyJwjL5rjXW7gyw

Repeat until the derived address matches one with a balance.

Legal & Ethical Notice

⚠️ This tool and analysis are for educational purposes only.

Do not use this knowledge to exploit wallets or access unauthorized funds.

It is intended for research, recovery of lost keys, or verifying wallet security.

Always comply with local laws and ethical standards.

References

https://www.unciphered.com/blog/randstorm-you-cant-patch-a-house-of-cards

https://jandemooij.nl/blog/math-random-and-32-bit-precision/

https://medium.com/@betable/tifu-by-using-math-random-f1c308c4fd9d

https://security.stackexchange.com/questions/84906/predicting-math-random-numbers

https://lwn.net/Articles/666407/

https://github.com/PwnFunction/v8-randomness-predictor

https://github.com/d0nutptr/v8_rand_buster

