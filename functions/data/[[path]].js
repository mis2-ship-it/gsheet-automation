// Serves /data/* only from the signed-in login's own data set.
// Cloudflare Access puts the verified login email in the header below.
// - listed in User Access: that login's stores only
// - any other @frozenbottle.in login: all stores
// - anyone else: refused
import ACCESS from "../_access.json";

const NO_STORE = { "Cache-Control": "private, no-cache" };

export async function onRequestGet(context) {
  const { request, env, params } = context;
  const email = (request.headers.get("Cf-Access-Authenticated-User-Email") || "").trim().toLowerCase();
  if (!email) return new Response("Sign-in required.", { status: 403 });

  let token;
  if (Object.prototype.hasOwnProperty.call(ACCESS.emails, email)) {
    token = ACCESS.emails[email];
    if (!token) return new Response("No stores are assigned to this login.", { status: 403 });
  } else if (email.endsWith("@" + ACCESS.domain)) {
    token = ACCESS.all;
  } else {
    return new Response("This login has no access.", { status: 403 });
  }

  const file = (Array.isArray(params.path) ? params.path : [params.path]).join("/");
  if (file === "whoami.json") {
    const body = JSON.stringify({ email, access: token === ACCESS.all ? "all stores" : "assigned stores" });
    return new Response(body, { headers: { "Content-Type": "application/json", ...NO_STORE } });
  }
  if (!/^[A-Za-z0-9_.\/-]+$/.test(file) || file.split("/").some(p => p === "" || p === "." || p === "..")) {
    return new Response("Not found.", { status: 404 });
  }

  const url = new URL(request.url);
  url.pathname = `/_g/${token}/${file}`;
  const res = await env.ASSETS.fetch(new Request(url, request));
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(NO_STORE)) headers.set(k, v);
  return new Response(res.body, { status: res.status, headers });
}
