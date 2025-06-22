use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::error::ErrorResponse;
use crate::error::error_resp_to_object_err;
use crate::client::{
    api_get_options::GetObjectOptions,
    credentials::{Credentials, Static, Value, SignatureType},
    transition_api::{ReaderImpl, ReadCloser},
    api_put_object::PutObjectOptions,
    api_remove::RemoveObjectOptions,
    transition_api::{Options, TransitionClient, TransitionCore,},
};
use rustfs_utils::path::SLASH_SEPARATOR;
use crate::tier::{
    tier_config::TierS3,
    warm_backend::{WarmBackend, WarmBackendGetOpts,}
};

pub struct WarmBackendS3 {
    pub client: Arc<TransitionClient>,
    pub core: TransitionCore,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

impl WarmBackendS3 {
    pub async fn new(conf: &TierS3, tier: &str) -> Result<Self, std::io::Error> {
        let u = match Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };

        if conf.aws_role_web_identity_token_file == "" && conf.aws_role_arn != "" || conf.aws_role_web_identity_token_file != "" && conf.aws_role_arn == "" {
            return Err(std::io::Error::other("both the token file and the role ARN are required"));
        }
        else if conf.access_key == "" && conf.secret_key != "" || conf.access_key != "" && conf.secret_key == "" {
            return Err(std::io::Error::other("both the access and secret keys are required"));
        }
        else if conf.aws_role && (conf.aws_role_web_identity_token_file != "" || conf.aws_role_arn != "" || conf.access_key != "" || conf.secret_key != "") {
            return Err(std::io::Error::other("AWS Role cannot be activated with static credentials or the web identity token file"));
        }
        else if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let mut creds: Credentials<Static>;

        if conf.access_key != "" && conf.secret_key != "" {
            //creds = Credentials::new_static_v4(conf.access_key, conf.secret_key, "");
            creds = Credentials::new(Static(Value {
                access_key_id:     conf.access_key.clone(),
                secret_access_key: conf.secret_key.clone(),
                session_token:     "".to_string(),
                signer_type: SignatureType::SignatureV4,
                ..Default::default()
            }));
        }
        else {
            return Err(std::io::Error::other("insufficient parameters for S3 backend authentication"));
        }
        let opts = Options {
            creds:     creds,
            secure:    u.scheme() == "https",
            //transport: GLOBAL_RemoteTargetTransport,
            ..Default::default()
        };
        let client = TransitionClient::new(&u.host().expect("err").to_string(), opts).await?;
        //client.set_appinfo(format!("s3-tier-{}", tier), ReleaseTag);

        let client = Arc::new(client);
        let core = TransitionCore(Arc::clone(&client));
        Ok(Self {
            client,
            core,
            bucket:        conf.bucket.clone(),
            prefix:        conf.prefix.clone().trim_matches('/').to_string(),
            storage_class: conf.storage_class.clone(),
        })
    }

    fn to_object_err(&self, err: ErrorResponse, params: Vec<&str>) -> std::io::Error {
        let mut object = "";
        if params.len() >= 1 {
            object = params.first().cloned().unwrap_or_default();
        }

        error_resp_to_object_err(err, vec![&self.bucket, &self.get_dest(object)])
    }

    pub fn get_dest(&self, object: &str) -> String {
        let mut dest_obj = object.to_string();
        if self.prefix != "" {
            dest_obj = format!("{}/{}", &self.prefix, object);
        }
        return dest_obj;
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendS3 {
    async fn put_with_meta(&self, object: &str, r: ReaderImpl, length: i64, meta: HashMap<String, String>) -> Result<String, std::io::Error> {
        let client = self.client.clone();
        let res = client.put_object(&self.bucket, &self.get_dest(object), r, length, &PutObjectOptions {
            send_content_md5: true,
            storage_class: self.storage_class.clone(),
            user_metadata: meta,
            ..Default::default()
        }).await?;
        Ok(res.version_id)
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let mut gopts = GetObjectOptions::default();

        if rv != "" {
            gopts.version_id = rv.to_string();
        }
        if opts.start_offset >= 0 && opts.length > 0 {
            if let Err(err) = gopts.set_range(opts.start_offset, opts.start_offset+opts.length-1) {
                return Err(std::io::Error::other(err));
            }
        }
        let c = TransitionCore(Arc::clone(&self.client));
        let (_, _, r) = c.get_object(&self.bucket, &self.get_dest(object), &gopts).await?;

        Ok(r)
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        let mut ropts = RemoveObjectOptions::default();
        if rv != "" {
            ropts.version_id = rv.to_string();
        }
        let client = self.client.clone();
        let err = client.remove_object(&self.bucket, &self.get_dest(object), ropts).await;
        Err(std::io::Error::other(err.expect("err")))
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        let result = self.core.list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1).await?;

        Ok(result.common_prefixes.len() > 0 || result.contents.len() > 0)
    }
}