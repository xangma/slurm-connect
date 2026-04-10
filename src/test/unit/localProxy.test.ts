import { describe, expect, it } from 'vitest';

import {
  buildLocalProxyControlPath,
  buildLocalProxyRemotePortCandidates,
  buildLocalProxyTunnelConfigKey,
  buildLocalProxyTunnelTarget,
  buildNoProxyValue,
  isHostAllowed,
  isLoopbackHost,
  isProxyAuthValid,
  normalizeProxyPort,
  parseProxyTarget,
  resolveRemoteBindConnectHost,
  splitHostPort
} from '../../utils/localProxy';

describe('localProxy utilities', () => {
  it('normalizes ports into the valid TCP range', () => {
    expect(normalizeProxyPort(8080.9)).toBe(8080);
    expect(normalizeProxyPort(-1)).toBe(0);
    expect(normalizeProxyPort(70000)).toBe(0);
  });

  it('builds deterministic remote-port candidates when given a custom random source', () => {
    const sequence = [0, 0.5, 0.75];
    let index = 0;
    const ports = buildLocalProxyRemotePortCandidates(62000, 3, () => sequence[index++ % sequence.length]!);
    expect(ports).toHaveLength(3);
    expect(new Set(ports).size).toBe(3);
    expect(ports[0]).toBe(62000);
  });

  it('recognizes loopback and allowed proxy targets', () => {
    expect(isLoopbackHost('localhost')).toBe(true);
    expect(isLoopbackHost('::ffff:7f00:1')).toBe(true);
    expect(isHostAllowed('127.0.0.1')).toBe(false);
    expect(isHostAllowed('cluster.example.com')).toBe(true);
  });

  it('parses host:port pairs including bracketed IPv6 literals', () => {
    expect(splitHostPort('[2001:db8::1]:8443')).toEqual({
      host: '2001:db8::1',
      port: 8443
    });
    expect(splitHostPort('example.com:80')).toEqual({
      host: 'example.com',
      port: 80
    });
  });

  it('builds stable no-proxy and tunnel metadata', () => {
    expect(buildNoProxyValue([' localhost ', 'example.com', 'localhost'])).toBe('localhost,example.com');
    expect(
      buildLocalProxyTunnelConfigKey(
        {
          user: 'alice',
          identityFile: '~/.ssh/id_ed25519',
          sshQueryConfigPath: '~/.ssh/config'
        },
        'login.example.com',
        '0.0.0.0',
        3128
      )
    ).toContain('"loginHost":"login.example.com"');
    expect(buildLocalProxyTunnelTarget({ user: 'alice' }, 'login.example.com')).toBe(
      'alice@login.example.com'
    );
    expect(buildLocalProxyControlPath('same-key')).toContain('slurm-connect-tunnel-');
    expect(resolveRemoteBindConnectHost('[::]')).toBe('127.0.0.1');
  });

  it('parses proxy targets from absolute and relative URLs', () => {
    expect(
      parseProxyTarget({
        url: '/resource?q=1',
        headers: {
          host: 'example.com:8080'
        }
      })
    ).toEqual({
      hostname: 'example.com',
      port: 8080,
      path: '/resource?q=1',
      isHttps: false,
      hostHeader: 'example.com:8080'
    });

    expect(
      parseProxyTarget({
        url: 'https://[2001:db8::1]:4443/api',
        headers: {}
      })
    ).toEqual({
      hostname: '2001:db8::1',
      port: 4443,
      path: '/api',
      isHttps: true,
      hostHeader: '[2001:db8::1]:4443'
    });
  });

  it('validates proxy basic-auth headers', () => {
    const auth = Buffer.from('proxy-user:secret').toString('base64');
    expect(
      isProxyAuthValid(
        {
          headers: {
            'proxy-authorization': `Basic ${auth}`
          }
        },
        'proxy-user',
        'secret'
      )
    ).toBe(true);

    expect(
      isProxyAuthValid(
        {
          headers: {
            'proxy-authorization': `Basic ${auth}`
          }
        },
        'proxy-user',
        'wrong'
      )
    ).toBe(false);
  });
});
