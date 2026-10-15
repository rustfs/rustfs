use aws_credential_types::Credentials as S3Credentials;
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use libvips::{VipsApp, VipsImage, ops};
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::Write, path::Path};
use tokio::io::AsyncReadExt;
use url::Url;
use zeroize::{Zeroize, Zeroizing};

const INPUT_PATH: &str = "/input/input.json";
const CREDENTIAL_PATH: &str = "/credentials/credential.json";
const OUTPUT_DIR: &str = "/output";
const INPUT_JSON_LIMIT: u64 = 64 * 1024;
const CREDENTIAL_JSON_LIMIT: u64 = 16 * 1024;
const SOURCE_LIMIT: usize = 32 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 16 * 1024 * 1024;
const MAX_DIMENSION: u32 = 10_000;
const MAX_PIXELS: u64 = 50_000_000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    platform: String,
    run: String,
    attempt: String,
    epoch: String,
    session: String,
    control_generation: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRef {
    bucket: String,
    key: String,
    version: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobInput {
    schema: String,
    attempt: Attempt,
    object: ObjectRef,
    #[serde(default)]
    parameters: Parameters,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Parameters {
    width: Option<u32>,
    height: Option<u32>,
    fit: Option<Fit>,
    format: Option<OutputFormat>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Fit {
    #[default]
    Inside,
    Cover,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum OutputFormat {
    #[default]
    Webp,
    Jpeg,
    Png,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobCredentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl Drop for JobCredentials {
    fn drop(&mut self) {
        self.access_key.zeroize();
        self.secret_key.zeroize();
        if let Some(token) = &mut self.session_token {
            token.zeroize();
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct JobResult<'a> {
    schema: &'static str,
    attempt: &'a Attempt,
    outcome: &'static str,
    artifacts: [ArtifactRef; 1],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRef {
    name: &'static str,
    file: &'static str,
}

#[derive(Clone, Copy, Debug)]
struct TransformSpec {
    width: u32,
    height: u32,
    fit: Fit,
    format: OutputFormat,
}

#[tokio::main]
async fn main() {
    if let Err(code) = run().await {
        eprintln!("image_plugin.rejected error_code={code}");
        std::process::exit(2);
    }
}

async fn run() -> Result<(), &'static str> {
    let (endpoint, region) = arguments()?;
    let input: JobInput =
        serde_json::from_slice(&read_bounded(INPUT_PATH, INPUT_JSON_LIMIT).await?).map_err(|_| "INPUT_INVALID")?;
    let spec = validate(&input)?;
    let mut secrets: JobCredentials = serde_json::from_slice(&read_bounded(CREDENTIAL_PATH, CREDENTIAL_JSON_LIMIT).await?)
        .map_err(|_| "CREDENTIALS_INVALID")?;
    if secrets.access_key.is_empty() || secrets.secret_key.is_empty() {
        return Err("CREDENTIALS_INVALID");
    }

    let downloaded = Zeroizing::new(download_source(&endpoint, &region, &input.object, &mut secrets).await?);
    let _vips = VipsApp::default("rustfs-image-plugin").map_err(|_| "IMAGE_DECODE_FAILED")?;
    let (filename, output) = transform(&downloaded, spec)?;
    let result = JobResult {
        schema: "rustfs.job/v2alpha1",
        attempt: &input.attempt,
        outcome: "ACCEPTED",
        artifacts: [ArtifactRef {
            name: "image",
            file: filename,
        }],
    };
    let result = serde_json::to_vec(&result).map_err(|_| "RESULT_INVALID")?;
    if output.len().checked_add(result.len()).is_none_or(|size| size > OUTPUT_LIMIT) {
        return Err("OUTPUT_LIMIT");
    }
    write_new(&Path::new(OUTPUT_DIR).join(filename), &output)?;
    write_new(&Path::new(OUTPUT_DIR).join("result.json"), &result)
}

fn arguments() -> Result<(String, String), &'static str> {
    let mut args = std::env::args().skip(1);
    let mut endpoint = None;
    let mut region = None;
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("ARGUMENT_INVALID")?;
        match arg.as_str() {
            "--endpoint" if endpoint.is_none() => endpoint = Some(value),
            "--region" if region.is_none() => region = Some(value),
            _ => return Err("ARGUMENT_INVALID"),
        }
    }
    let endpoint = endpoint.ok_or("ARGUMENT_INVALID")?;
    let parsed = Url::parse(&endpoint).map_err(|_| "ENDPOINT_INVALID")?;
    let loopback_http = parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| host.parse::<std::net::IpAddr>().is_ok_and(|address| address.is_loopback()));
    if region.as_deref().is_some_and(str::is_empty)
        || (parsed.scheme() != "https" && !loopback_http)
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("ENDPOINT_INVALID");
    }
    Ok((endpoint, region.ok_or("ARGUMENT_INVALID")?))
}

fn validate(input: &JobInput) -> Result<TransformSpec, &'static str> {
    if input.schema != "rustfs.job/v2alpha1"
        || input.object.bucket.is_empty()
        || input.object.bucket.len() > 256
        || input.object.key.is_empty()
        || input.object.key.len() > 1024
    {
        return Err("INPUT_INVALID");
    }
    let version = input.object.version.as_deref().ok_or("VERSION_REQUIRED")?;
    if version.is_empty() || version == "null" || version.len() > 1024 {
        return Err("VERSION_REQUIRED");
    }
    let parameters = &input.parameters;
    let width = parameters.width.unwrap_or(320);
    let height = parameters.height.unwrap_or(240);
    if width == 0
        || height == 0
        || width > MAX_DIMENSION
        || height > MAX_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_PIXELS
    {
        return Err("DIMENSIONS_INVALID");
    }
    Ok(TransformSpec {
        width,
        height,
        fit: parameters.fit.unwrap_or_default(),
        format: parameters.format.unwrap_or_default(),
    })
}

async fn read_bounded(path: &str, limit: u64) -> Result<Vec<u8>, &'static str> {
    let file = tokio::fs::File::open(path).await.map_err(|_| "INPUT_READ_FAILED")?;
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "INPUT_READ_FAILED")?;
    if bytes.len() as u64 > limit {
        return Err("INPUT_LIMIT");
    }
    Ok(bytes)
}

async fn download_source(
    endpoint: &str,
    region: &str,
    object: &ObjectRef,
    secrets: &mut JobCredentials,
) -> Result<Vec<u8>, &'static str> {
    let token = secrets.session_token.take();
    let credentials = S3Credentials::new(
        std::mem::take(&mut secrets.access_key),
        std::mem::take(&mut secrets.secret_key),
        token,
        None,
        "rustfs-image-plugin",
    );
    let config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .endpoint_url(endpoint)
        .region(Region::new(region.to_owned()))
        .credentials_provider(credentials)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(config);
    let version = object.version.as_deref().ok_or("VERSION_REQUIRED")?;
    let response = client
        .get_object()
        .bucket(&object.bucket)
        .key(&object.key)
        .version_id(version)
        .send()
        .await
        .map_err(|_| "SOURCE_READ_FAILED")?;
    if response
        .content_length()
        .is_some_and(|size| size < 0 || size > SOURCE_LIMIT as i64)
    {
        return Err("SOURCE_LIMIT");
    }
    let mut stream: ByteStream = response.body;
    let mut source = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "SOURCE_READ_FAILED")?;
        if source.len().saturating_add(chunk.len()) > SOURCE_LIMIT {
            return Err("SOURCE_LIMIT");
        }
        source.extend_from_slice(&chunk);
    }
    if source.is_empty() {
        return Err("SOURCE_EMPTY");
    }
    Ok(source)
}

fn transform(source: &[u8], spec: TransformSpec) -> Result<(&'static str, Vec<u8>), &'static str> {
    let decoded = VipsImage::new_from_buffer(source, "").map_err(|_| "IMAGE_DECODE_FAILED")?;
    let upright = ops::autorot(&decoded).map_err(|_| "IMAGE_DECODE_FAILED")?;
    let source_width = u32::try_from(upright.get_width()).map_err(|_| "IMAGE_DECODE_FAILED")?;
    let source_height = u32::try_from(upright.get_height()).map_err(|_| "IMAGE_DECODE_FAILED")?;
    if source_width == 0 || source_height == 0 || u64::from(source_width) * u64::from(source_height) > MAX_PIXELS {
        return Err("SOURCE_DIMENSIONS_INVALID");
    }
    let width_scale = f64::from(spec.width) / f64::from(source_width);
    let height_scale = f64::from(spec.height) / f64::from(source_height);
    let scale = match spec.fit {
        Fit::Inside => width_scale.min(height_scale),
        Fit::Cover => width_scale.max(height_scale),
    }
    .min(1.0);
    let resized = if scale < 1.0 {
        ops::resize(&upright, scale).map_err(|_| "IMAGE_DECODE_FAILED")?
    } else {
        upright
    };
    let image = if matches!(spec.fit, Fit::Cover) {
        let resized_width = u32::try_from(resized.get_width()).map_err(|_| "IMAGE_DECODE_FAILED")?;
        let resized_height = u32::try_from(resized.get_height()).map_err(|_| "IMAGE_DECODE_FAILED")?;
        let target_ratio = f64::from(spec.width) / f64::from(spec.height);
        let resized_ratio = f64::from(resized_width) / f64::from(resized_height);
        let (crop_width, crop_height) = if resized_ratio > target_ratio {
            (
                ((f64::from(resized_height) * target_ratio).round() as u32)
                    .min(spec.width)
                    .max(1),
                resized_height.min(spec.height),
            )
        } else {
            (
                resized_width.min(spec.width),
                ((f64::from(resized_width) / target_ratio).round() as u32)
                    .min(spec.height)
                    .max(1),
            )
        };
        ops::extract_area(
            &resized,
            ((resized_width - crop_width) / 2) as i32,
            ((resized_height - crop_height) / 2) as i32,
            crop_width as i32,
            crop_height as i32,
        )
        .map_err(|_| "IMAGE_DECODE_FAILED")?
    } else {
        resized
    };
    let (filename, bytes) = match spec.format {
        OutputFormat::Png => ("image.png", ops::pngsave_buffer(&image)),
        OutputFormat::Jpeg => ("image.jpg", ops::jpegsave_buffer(&image)),
        OutputFormat::Webp => ("image.webp", ops::webpsave_buffer(&image)),
    };
    let bytes = bytes.map_err(|_| "IMAGE_ENCODE_FAILED")?;
    Ok((filename, bytes))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), &'static str> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| "OUTPUT_WRITE_FAILED")?;
    file.write_all(bytes).map_err(|_| "OUTPUT_WRITE_FAILED")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parameters(fit: Fit) -> TransformSpec {
        TransformSpec {
            width: 20,
            height: 20,
            fit,
            format: OutputFormat::Png,
        }
    }

    fn png_40_by_20() -> Vec<u8> {
        let mut pixels = Vec::new();
        for y in 0..20u8 {
            for x in 0..40u8 {
                pixels.extend_from_slice(&[x * 6, y * 12, 80]);
            }
        }
        let image = VipsImage::new_from_memory_copy(&pixels, 40, 20, 3, ops::BandFormat::Uchar).unwrap();
        ops::pngsave_buffer(&image).unwrap()
    }

    #[test]
    fn cover_output_is_exact_dimensions() {
        let _app = VipsApp::default("rustfs-image-plugin-tests").unwrap();
        let source = png_40_by_20();
        let _decoded = VipsImage::new_from_buffer(&source, "").unwrap();
        let (file, bytes) = transform(&source, parameters(Fit::Cover)).unwrap();
        assert_eq!(file, "image.png");
        let image = VipsImage::new_from_buffer(&bytes, "").unwrap();
        assert_eq!((image.get_width(), image.get_height()), (20, 20));

        let (file, bytes) = transform(&source, parameters(Fit::Inside)).unwrap();
        assert_eq!(file, "image.png");
        let image = VipsImage::new_from_buffer(&bytes, "").unwrap();
        assert_eq!((image.get_width(), image.get_height()), (20, 10));

        for format in [OutputFormat::Jpeg, OutputFormat::Webp] {
            let mut spec = parameters(Fit::Inside);
            spec.format = format;
            let (file, bytes) = transform(&source, spec).unwrap();
            assert!(matches!(file, "image.jpg" | "image.webp"));
            let image = VipsImage::new_from_buffer(&bytes, "").unwrap();
            assert_eq!((image.get_width(), image.get_height()), (20, 10));
        }
    }

    #[test]
    fn omitted_parameters_use_documented_defaults() {
        let input: JobInput = serde_json::from_str(
            r#"{"schema":"rustfs.job/v2alpha1","attempt":{"platform":"p","run":"r","attempt":"a","epoch":"1","session":"s","control_generation":"g"},"object":{"bucket":"images","key":"photo.jpg","version":"v1"}}"#,
        )
        .unwrap();
        let spec = validate(&input).unwrap();
        assert_eq!((spec.width, spec.height), (320, 240));
        assert!(matches!(spec.fit, Fit::Inside));
        assert!(matches!(spec.format, OutputFormat::Webp));
    }

    #[test]
    fn unversioned_and_null_sentinel_inputs_are_rejected() {
        let input = JobInput {
            schema: "rustfs.job/v2alpha1".into(),
            attempt: Attempt {
                platform: "p".into(),
                run: "r".into(),
                attempt: "a".into(),
                epoch: "1".into(),
                session: "s".into(),
                control_generation: "g".into(),
            },
            object: ObjectRef {
                bucket: "bucket".into(),
                key: "image.png".into(),
                version: None,
            },
            parameters: Parameters::default(),
        };
        assert_eq!(validate(&input).unwrap_err(), "VERSION_REQUIRED");
        let input = JobInput {
            object: ObjectRef {
                bucket: "bucket".into(),
                key: "image.png".into(),
                version: Some("null".into()),
            },
            ..input
        };
        assert_eq!(validate(&input).unwrap_err(), "VERSION_REQUIRED");
    }

    #[tokio::test]
    async fn source_read_requests_the_exact_version_id() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0; 1024];
                let count = stream.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..count]);
                if count == 0 || request.windows(4).any(|part| part == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.lines().next().unwrap().contains("versionId=version-42"));
            stream
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let object = ObjectRef {
            bucket: "bucket".into(),
            key: "image.png".into(),
            version: Some("version-42".into()),
        };
        let mut secrets = JobCredentials {
            access_key: "test-access".into(),
            secret_key: "test-secret".into(),
            session_token: None,
        };
        assert_eq!(
            download_source(&endpoint, "us-east-1", &object, &mut secrets)
                .await
                .unwrap_err(),
            "SOURCE_READ_FAILED"
        );
        server.await.unwrap();
    }
}
