(ns rustfs.s3
  "Path-style S3 client for RustFS 1.0.7.

  Signing, retries, and XML are the AWS SDK. This namespace only decides which
  calls are definite and which are indeterminate, and maps object bodies onto
  the register values Elle and Knossos see. Listings are not register reads."
  (:require [clojure.string :as str])
  (:import [java.net URI]
           [java.time Duration]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials StaticCredentialsProvider]
           [software.amazon.awssdk.core.exception SdkClientException]
           [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
           [software.amazon.awssdk.core.retry RetryPolicy]
           [software.amazon.awssdk.core.sync RequestBody]
           [software.amazon.awssdk.http.urlconnection UrlConnectionHttpClient]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model
            AbortMultipartUploadRequest
            BucketAlreadyExistsException
            BucketAlreadyOwnedByYouException
            BucketVersioningStatus
            CompleteMultipartUploadRequest
            CompletedMultipartUpload
            CompletedPart
            CreateBucketRequest
            CreateMultipartUploadRequest
            DeleteObjectRequest
            GetObjectRequest
            ListObjectsV2Request
            NoSuchKeyException
            PutBucketVersioningRequest
            PutObjectRequest
            S3Exception
            UploadPartRequest
            VersioningConfiguration]))

(def ^:private call-timeout (Duration/ofSeconds 15))

(defn quoted-etag
  "If-Match wants the quoted S3 ETag. Multipart ETags keep their `-N` suffix."
  [etag]
  (let [etag (some-> etag str str/trim)]
    (cond
      (str/blank? etag) nil
      (str/starts-with? etag "\"") etag
      :else (str "\"" etag "\""))))

(defn etag-identity
  "Compare ETags without quotes or a weak-validator prefix."
  [etag]
  (some-> etag
          str
          str/trim
          (str/replace #"^W/" "")
          (str/replace #"^\"|\"$" "")
          not-empty))

(defn classify
  "Turn a thrown S3 or transport failure into `{:definite? bool :absent? bool :rejected? bool :error keyword}`."
  [^Throwable t]
  (cond
    (instance? NoSuchKeyException t)
    {:definite? true :absent? true :error :not-found}

    (instance? S3Exception t)
    (let [^S3Exception ex t
          status (.statusCode ex)
          code (some-> ex .awsErrorDetails .errorCode)]
      (cond
        (= 404 status)
        {:definite? true :absent? true :error (keyword (or code "not-found"))}

        (#{400 403 405 409 412 416} status)
        {:definite? true :rejected? true :error (keyword (or code "rejected")) :status status}

        :else
        {:definite? false :error (keyword (or code "server-error")) :status status}))

    (instance? SdkClientException t)
    {:definite? false :error :transport}

    :else
    {:definite? false :error :unknown}))

(defn- throw-classified!
  [^Throwable t]
  (throw (ex-info (.getMessage t) (classify t) t)))

(defn connect
  [{:keys [endpoint access-key secret-key]}]
  (let [http (.. (UrlConnectionHttpClient/builder)
                 (connectionTimeout (Duration/ofSeconds 3))
                 (socketTimeout call-timeout))
        retry (.. (RetryPolicy/builder)
                  (numRetries 0))
        override (.. (ClientOverrideConfiguration/builder)
                     (apiCallTimeout call-timeout)
                     (apiCallAttemptTimeout call-timeout)
                     (retryPolicy (.build retry)))]
    (.. (S3Client/builder)
        (httpClientBuilder http)
        (overrideConfiguration (.build override))
        (endpointOverride (URI/create endpoint))
        (region Region/US_EAST_1)
        (credentialsProvider
         (StaticCredentialsProvider/create
          (AwsBasicCredentials/create access-key secret-key)))
        (forcePathStyle true)
        (build))))

(defn- key-str
  [k]
  (str "k-" k))

(defn- body-str
  [value]
  (str value))

(defn- parse-body
  [body]
  (or (parse-long (str/trim body))
      [::unreadable body]))

(defn- result-meta
  [response size]
  {:etag (.eTag response)
   :version-id (.versionId response)
   :size size})

(defn ensure-bucket!
  [^S3Client client bucket]
  (try
    (.createBucket client (.. (CreateBucketRequest/builder) (bucket bucket) (build)))
    :created
    (catch BucketAlreadyOwnedByYouException _
      :exists)
    (catch BucketAlreadyExistsException _
      :exists)
    (catch S3Exception e
      (let [info (classify e)]
        (if (and (:definite? info) (:rejected? info)
                 (#{"BucketAlreadyOwnedByYou" "BucketAlreadyExists"}
                  (some-> e .awsErrorDetails .errorCode)))
          :exists
          (throw-classified! e))))
    (catch SdkClientException e
      (throw-classified! e))))

(defn enable-versioning!
  [^S3Client client bucket]
  (try
    (.putBucketVersioning
     client
     (.. (PutBucketVersioningRequest/builder)
         (bucket bucket)
         (versioningConfiguration
          (.. (VersioningConfiguration/builder)
              (status BucketVersioningStatus/ENABLED)
              (build)))
         (build)))
    :enabled
    (catch Throwable t
      (throw-classified! t))))

(defn- put-object!
  [^S3Client client bucket key body {:keys [if-match if-none-match]}]
  (let [req (-> (PutObjectRequest/builder)
                (.bucket bucket)
                (.key key)
                (cond->
                 if-match (.ifMatch (quoted-etag if-match))
                 if-none-match (.ifNoneMatch if-none-match))
                (.build))]
    (.putObject client req (RequestBody/fromString body))))

(defn- abort-upload!
  [^S3Client client bucket key upload-id]
  (try
    (.abortMultipartUpload
     client
     (-> (AbortMultipartUploadRequest/builder)
         (.bucket bucket)
         (.key key)
         (.uploadId upload-id)
         (.build)))
    (catch Throwable _)))

(defn- put-multipart!
  "One small part, then Complete. A lost Complete response is indeterminate, so
  this does not abort after Complete has been attempted."
  [^S3Client client bucket key body]
  (let [created (.createMultipartUpload
                 client
                 (-> (CreateMultipartUploadRequest/builder)
                     (.bucket bucket)
                     (.key key)
                     (.build)))
        upload-id (.uploadId created)
        part (try
               (.uploadPart
                client
                (-> (UploadPartRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.uploadId upload-id)
                    (.partNumber (int 1))
                    (.build))
                (RequestBody/fromString body))
               (catch Throwable t
                 (abort-upload! client bucket key upload-id)
                 (throw-classified! t)))
        completed-part (-> (CompletedPart/builder)
                           (.partNumber (int 1))
                           (.eTag (.eTag part))
                           (.build))
        upload (-> (CompletedMultipartUpload/builder)
                   (.parts ^java.util.Collection [completed-part])
                   (.build))]
    (try
      (.completeMultipartUpload
       client
       (-> (CompleteMultipartUploadRequest/builder)
           (.bucket bucket)
           (.key key)
           (.uploadId upload-id)
           (.multipartUpload upload)
           (.build)))
      (catch Throwable t
        (throw-classified! t)))))

(defn write-value!
  "Write `value`. A nil value deletes the current object. `opts` may contain
  `:multipart?`, `:if-match`, or `:if-none-match`."
  [^S3Client client bucket k value opts]
  (let [key (key-str k)]
    (try
      (if (nil? value)
        (let [resp (.deleteObject
                    client
                    (.. (DeleteObjectRequest/builder) (bucket bucket) (key key) (build)))]
          {:etag nil :version-id (.versionId resp) :size 0 :deleted? true})
        (let [body (body-str value)
              resp (if (:multipart? opts)
                     (put-multipart! client bucket key body)
                     (put-object! client bucket key body opts))]
          (result-meta resp (count (.getBytes body "UTF-8")))))
      (catch clojure.lang.ExceptionInfo e
        (throw e))
      (catch Throwable t
        (throw-classified! t)))))

(defn read-value
  "Return `{:value v :etag ... :version-id ... :size ...}`. A missing key is
  `{:value nil}` and is a successful read, not a failure."
  [^S3Client client bucket k]
  (try
    (with-open [in (.getObject
                    client
                    (.. (GetObjectRequest/builder)
                        (bucket bucket)
                        (key (key-str k))
                        (build)))]
      (let [resp (.response in)
            body (slurp in)]
        {:value (parse-body body)
         :etag (.eTag resp)
         :version-id (.versionId resp)
         :size (count (.getBytes body "UTF-8"))}))
    (catch NoSuchKeyException _
      {:value nil :etag nil :version-id nil :size 0 :absent? true})
    (catch Throwable t
      (let [info (classify t)]
        (if (:absent? info)
          {:value nil :etag nil :version-id nil :size 0 :absent? true}
          (throw-classified! t))))))

(defn list-prefix
  "List current objects under `k-`. This is not a linearizable read."
  [^S3Client client bucket]
  (try
    (loop [token nil acc []]
        (let [resp (.listObjectsV2
                  client
                  (-> (ListObjectsV2Request/builder)
                      (.bucket bucket)
                      (.prefix "k-")
                      (.maxKeys (int 1000))
                      (cond-> token (.continuationToken token))
                      (.build)))
            page (mapv (fn [obj]
                         {:key (.key obj)
                          :etag (.eTag obj)
                          :size (some-> (.size obj) long)})
                       (.contents resp))
            acc (into acc page)]
        (if (.isTruncated resp)
          (recur (.nextContinuationToken resp) acc)
          acc)))
    (catch Throwable t
      (throw-classified! t))))

(defn close!
  [^S3Client client]
  (when client
    (.close client)))
