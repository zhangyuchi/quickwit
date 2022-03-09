// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    println!("cargo:rerun-if-changed=proto/search_api.proto");
    println!("cargo:rerun-if-changed=SERVICE.rs.tmpl");

    let mut prost_config = prost_build::Config::default();
    // prost_config.type_attribute("LeafSearchResponse", "#[derive(Default)]");
    prost_config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(Serialize, Deserialize)]\n#[serde(rename_all = \"camelCase\")]",
        )
        .format(true)
        .out_dir("src/")
        .compile_with_config(
            prost_config,
            &["./proto/cluster.proto", "./proto/search_api.proto"],
            &["./proto"],
        )?;

    prost_build::Config::default()
        .service_generator(Box::new(QuickwitServiceGenerator))
        .out_dir("service/")
        .compile_protos( &[
                "./proto/cluster.proto",
                "./proto/search_api.proto",
        ],
        &["./proto"])?;
    Ok(())
}

use serde::Serialize;
/// A service descriptor.
#[derive(Serialize)]
struct Service {
    /// The service name in Rust style.
    pub service_name: String,
    /// The service methods.
    pub methods_streaming: Vec<Method>,
    pub methods_non_streaming: Vec<Method>,
}

#[derive(Serialize)]
struct Method {
    /// The name of the method in Rust style.
    pub name: String,
    pub input_type: String,
    pub output_type: String,
    // pub client_streaming: bool,
    // pub server_streaming: bool,
}

impl From<prost_build::Method> for Method {
    fn from(method: prost_build::Method) -> Self {
        Method {
            name: method.name,
            input_type: method.input_type,
            output_type: method.output_type,
            // server_streaming: method.server_streaming
        }
    }
}

impl From<prost_build::Service> for Service {
    fn from(service: prost_build::Service) -> Self {
        let (streaming_methods, non_streaming_methods): (Vec<_>, Vec<_>) = service.methods
            .into_iter()
            .partition(|method| method.server_streaming);
        Service {
            service_name: service.name,
            methods_streaming: streaming_methods.into_iter().map(Method::from).collect(),
            methods_non_streaming: non_streaming_methods.into_iter().map(Method::from).collect(),
        }
    }
}

const SERVICE_TEMPLATE: &str = include_str!("SERVICE.rs.tmpl");

struct QuickwitServiceGenerator;

impl prost_build::ServiceGenerator for QuickwitServiceGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let service = Service::from(service);
        let tmpl = mustache::compile_str(SERVICE_TEMPLATE).unwrap();
        buf.push_str(&tmpl.render_to_string(&service).unwrap());
    }
}
