package com.acme.fw

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}
import com.fasterxml.jackson.databind.ObjectMapper   // already inside Spark's jars

/*
 * VaultClient.scala  --  fetches secrets from HashiCorp Vault (KV v2)
 *
 * Step 1: log in to Vault using the pod's Kubernetes service-account token (no password needed)
 * Step 2: read the secret at the given path and return it as a Map  (e.g. "username" -> "...")
 *
 * Env vars (not secret, set by the wrapper):
 *   VAULT_ADDR   = https://vault.acme.com:8200
 *   VAULT_ROLE   = spark-driver          (Vault role bound to the pod's service account)
 *   VAULT_MOUNT  = secret                (optional, KV v2 mount name, default "secret")
 *   VAULT_TOKEN  = only for local testing; if set, step 1 is skipped
 */
object VaultClient {

  private val http   = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()

  // Step 1: get a Vault token
  private def login(): String = {
    if (sys.env.contains("VAULT_TOKEN")) return sys.env("VAULT_TOKEN")

    val addr = sys.env("VAULT_ADDR")
    val role = sys.env("VAULT_ROLE")
    val jwt  = new String(
      Files.readAllBytes(Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token"))).trim

    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$addr/v1/auth/kubernetes/login"))
      .POST(HttpRequest.BodyPublishers.ofString(s"""{"role":"$role","jwt":"$jwt"}"""))
      .build()

    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new RuntimeException(s"Vault login failed, HTTP ${response.statusCode()}")

    mapper.readTree(response.body()).get("auth").get("client_token").asText()
  }

  // Step 2: read one secret, e.g. getSecret("pipelines/teradata")
  def getSecret(path: String): Map[String, String] = {
    val addr  = sys.env("VAULT_ADDR")
    val mount = sys.env.getOrElse("VAULT_MOUNT", "secret")

    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$addr/v1/$mount/data/$path"))
      .header("X-Vault-Token", login())
      .GET()
      .build()

    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new RuntimeException(s"Vault read failed for '$path', HTTP ${response.statusCode()}")

    val fields = mapper.readTree(response.body()).get("data").get("data").fields()
    var secret = Map[String, String]()
    while (fields.hasNext) {
      val entry = fields.next()
      secret += (entry.getKey -> entry.getValue.asText())
    }
    secret          // never print or log this map
  }
}
