Mox.defmock(Polyn.FileMock, for: Polyn.FileBehaviour)
Mox.stub_with(Polyn.FileMock, Polyn.FileStub)

Mox.defmock(Polyn.CodeMock, for: Polyn.CodeBehaviour)
Mox.stub_with(Polyn.CodeMock, Polyn.CodeStub)

ExUnit.start()
