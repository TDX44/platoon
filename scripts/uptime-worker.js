// Outside-in uptime check for Platoon Manager: a Cloudflare Worker on a
// five-minute cron (account PlatoonManager, script "platoon-uptime").
//
// scripts/healthcheck.sh covers everything it can see from prodsrv02, but a
// dead prodsrv02 — power, network, Docker — cannot email about itself. This
// runs on Cloudflare, so it does. Two failures in a row before alerting, so a
// deploy's few seconds of restart is not a page; one mail per outage, one on
// recovery. State lives in the UPTIME KV namespace.
//
// Bindings: UPTIME (KV), RESEND_API_KEY (secret), NOTIFY_FROM, ALERT_EMAIL.
// Deployed through the Cloudflare API; this file is the source of record.

const TARGETS = ['https://app.platoonmanager.com/api/auth/config', 'https://platoonmanager.com/'];
const FAILS_BEFORE_ALERT = 2;

async function probe(url) {
  try {
    const r = await fetch(url, { cf: { cacheTtl: 0 }, redirect: 'manual', signal: AbortSignal.timeout(15000) });
    return r.status === 200 ? null : `${url} answered ${r.status}`;
  } catch (e) {
    return `${url} did not answer: ${e.message}`;
  }
}

async function mail(env, subject, text) {
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: env.NOTIFY_FROM, to: [env.ALERT_EMAIL], subject, text }),
  });
  if (!r.ok) throw new Error(`resend ${r.status}`);
}

export default {
  async scheduled(event, env) {
    const problems = (await Promise.all(TARGETS.map(probe))).filter(Boolean);
    const fails = Number(await env.UPTIME.get('fails')) || 0;
    const alerted = (await env.UPTIME.get('alerted')) === '1';
    if (!problems.length) {
      if (alerted) await mail(env, 'Platoon Manager is reachable again', `Checked from Cloudflare at ${new Date().toISOString()}.`);
      // Only write on a change: KV's free tier allows 1,000 writes a day.
      if (fails) await env.UPTIME.put('fails', '0');
      if (alerted) await env.UPTIME.put('alerted', '0');
      return;
    }
    await env.UPTIME.put('fails', String(fails + 1));
    if (fails + 1 >= FAILS_BEFORE_ALERT && !alerted) {
      await mail(env, `Platoon Manager unreachable: ${problems[0]}`,
        `${problems.join('\n')}\n\nChecked from Cloudflare (outside the homelab) at ${new Date().toISOString()}.\n` +
        'If prodsrv02 is up, see its own health-check mail; if not, the host itself is down.');
      await env.UPTIME.put('alerted', '1');
    }
  },
};
