use pyo3::prelude::*;
use pyo3::types::PyTuple;

fn main() -> PyResult<()> {
    let arg1 = "a";

    let py_app = include_str!("/Users/drws/Workspace/pydozer/main.py");
    let from_python = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        // PyModule::from_code(py, py_foo, "utils.foo", "utils.foo")?;
        let app: Py<PyAny> = PyModule::from_code(py, py_app, "", "")?
            .getattr("execute")?
            .into();

        let args = PyTuple::new(py, [arg1]);
        app.call1(py, args)
    });

    println!("py: {}", from_python?);
    Ok(())
}
