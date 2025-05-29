# BitcoinJS-lib v0.1.3 Entropy Vulnerability Analysis

## Overview
This repository contains an analysis of a critical entropy vulnerability in BitcoinJS-lib v0.1.3 that affected numerous Bitcoin exchanges and web wallets between 2010 and 2015. The vulnerability stemmed from the improper implementation of cryptographic randomness generation in browsers that lacked proper entropy sources.

## Technical Background

### Vulnerability Source
The vulnerability originated from browsers' failure to implement `window.crypto.getRandomValues()`, causing the library to fall back to the insecure `Math.random()` function for entropy generation. This significantly compromised the cryptographic security of generated private keys.

### Entropy Generation Implementation

#### Initial Pool Generation
```javascript
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
```

The code initializes a 256-byte pool using 16-bit integers derived from `Math.random()`. Each value is split into high and low bytes.

#### Time-Based Entropy Addition
```javascript
this.seedInt = function (x) {
  this.pool[this.pptr++] ^= x & 255;
  this.pool[this.pptr++] ^= (x >> 8) & 255;
  this.pool[this.pptr++] ^= (x >> 16) & 255;
  this.pool[this.pptr++] ^= (x >> 24) & 255;
  if (this.pptr >= this.poolSize) this.pptr -= this.poolSize;
};
```

A timestamp is XORed into the pool to add additional entropy, though this proved insufficient due to the predictability of the seed time.

### Cryptographic Processing
The seeded pool is processed using an RC4 stream cipher (Arcfour):

```javascript
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
```

## Vulnerability Analysis

### Predictability
The vulnerability stems from the deterministic nature of `Math.random()` in V8-based browsers, making it possible to predict outputs using specialized tools:
- v8-randomness-predictor
- v8_rand_buster

### Real-World Impact
Projects like Coinpunk implemented this vulnerable entropy mechanism, allowing attackers to recreate private keys if the seed time was known.

## Detection and Analysis

### Identifying Affected Wallets
To identify potentially vulnerable wallets:
1. Determine the timestamp of the first transaction
2. Convert to Unix time
3. Analyze the entropy generation process

Example:
```
Address: 1NUhcfvRthmvrHf1PAJKe5uEzBGK44ASBD
First Transaction: 2014-03-16 23:48:51 GMT-7
Unix Time: 1395038931000
```

### Seed Scanning Methodology
The vulnerability allows for systematic scanning of potential seed values to identify compromised wallets.

## Legal and Ethical Considerations

⚠️ **Important Notice**

This analysis and associated tools are provided for:
- Educational purposes
- Security research
- Recovery of lost keys
- Wallet security verification

**Prohibited Uses:**
- Exploitation of vulnerable wallets
- Unauthorized access to funds
- Any illegal activities

Users must comply with all applicable laws and ethical standards.

## References

1. [Unciphered Blog: Randstorm Analysis](https://www.unciphered.com/blog/randstorm-you-cant-patch-a-house-of-cards)
2. [Math.random and 32-bit Precision](https://jandemooij.nl/blog/math-random-and-32-bit-precision/)
3. [Betable's Math.random Analysis](https://medium.com/@betable/tifu-by-using-math-random-f1c308c4fd9d)
4. [Security Stack Exchange Discussion](https://security.stackexchange.com/questions/84906/predicting-math-random-numbers)
5. [LWN Article on Randomness](https://lwn.net/Articles/666407/)
6. [V8 Randomness Predictor](https://github.com/PwnFunction/v8-randomness-predictor)
7. [V8 Rand Buster](https://github.com/d0nutptr/v8_rand_buster)

