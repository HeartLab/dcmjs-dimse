import { association, Client, constants, Dataset, requests, responses, Scp, Server } from './..';
import { Socket } from 'net';
import path from 'path';

const { CEchoRequest, CFindRequest, CStoreRequest } = requests;
const { CEchoResponse, CFindResponse, CStoreResponse } = responses;
const {
  PresentationContextResult,
  RejectReason,
  RejectResult,
  RejectSource,
  SopClass,
  Status,
  StorageClass,
  TransferSyntax,
  UserIdentityType,
} = constants;

function performCEcho(
  host: string,
  port: number,
  callingAeTitle: string,
  calledAeTitle: string,
  opts: {
    userIdentity?: {
      type?: number;
      positiveResponseRequested?: boolean;
      primaryField?: string;
      secondaryField?: string;
    };
  }
) {
  const client = new Client();
  const request = new CEchoRequest();
  request.on('response', (response) => {
    if (response.getStatus() === Status.Success) {
      console.log('Happy!');
    }
  });
  client.addRequest(request);
  client.send(host, port, callingAeTitle, calledAeTitle, opts);
}

function performCFindStudy(
  host: string,
  port: number,
  callingAeTitle: string,
  calledAeTitle: string,
  opts: {
    userIdentity?: {
      type?: number;
      positiveResponseRequested?: boolean;
      primaryField?: string;
      secondaryField?: string;
    };
  }
) {
  const client = new Client();
  const request = CFindRequest.createStudyFindRequest({ PatientName: '*' });
  request.on('response', (response) => {
    if (response.getStatus() === Status.Pending && response.hasDataset()) {
      console.log(response.getDataset());
    }
  });
  client.addRequest(request);
  client.send(host, port, callingAeTitle, calledAeTitle, opts);
}

function performCFindMwl(
  host: string,
  port: number,
  callingAeTitle: string,
  calledAeTitle: string,
  opts: {
    userIdentity?: {
      type?: number;
      positiveResponseRequested?: boolean;
      primaryField?: string;
      secondaryField?: string;
    };
  }
) {
  const client = new Client();
  const request = CFindRequest.createWorklistFindRequest({ PatientName: '*' });
  request.on('response', (response) => {
    if (response.getStatus() === Status.Pending && response.hasDataset()) {
      console.log(response.getDataset());
    }
  });
  client.addRequest(request);
  client.send(host, port, callingAeTitle, calledAeTitle, opts);
}

function performCStore(
  host: string,
  port: number,
  callingAeTitle: string,
  calledAeTitle: string,
  opts: {
    userIdentity?: {
      type?: number;
      positiveResponseRequested?: boolean;
      primaryField?: string;
      secondaryField?: string;
    };
  }
) {
  const rootPath = process.cwd();
  const elePath = path.join(rootPath, 'datasets', 'ele.dcm');
  const j2kPath = path.join(rootPath, 'datasets', 'j2k.dcm');
  const srPath = path.join(rootPath, 'datasets', 'sr.dcm');
  const pdfPath = path.join(rootPath, 'datasets', 'pdf.dcm');

  const client = new Client();
  client.addRequest(new CStoreRequest(elePath));
  client.addRequest(new CStoreRequest(j2kPath));
  client.addRequest(new CStoreRequest(srPath));
  client.addRequest(new CStoreRequest(pdfPath));
  client.send(host, port, callingAeTitle, calledAeTitle, opts);
}

class ExampleScp extends Scp {
  association: association.Association | undefined;

  constructor(socket: Socket, opts: any) {
    super(socket, opts);
    this.association = undefined;
  }

  associationRequested(association: association.Association) {
    this.association = association;

    // Evaluate calling/called AET and reject association, if needed
    if (this.association.getCallingAeTitle() !== 'SCU') {
      this.sendAssociationReject(
        RejectResult.Permanent,
        RejectSource.ServiceUser,
        RejectReason.CallingAeNotRecognized
      );
      return;
    }

    // Evaluate user identity and reject association, if needed
    if (
      this.association.getNegotiateUserIdentity() &&
      this.association.getUserIdentityPositiveResponseRequested()
    ) {
      if (
        this.association.getUserIdentityType() === UserIdentityType.UsernameAndPasscode &&
        this.association.getUserIdentityPrimaryField() === 'Username' &&
        this.association.getUserIdentitySecondaryField() === 'Password'
      ) {
        this.association.setUserIdentityServerResponse('');
        this.association.setNegotiateUserIdentityServerResponse(true);
      } else {
        this.sendAssociationReject(
          RejectResult.Permanent,
          RejectSource.ServiceUser,
          RejectReason.NoReasonGiven
        );
        return;
      }
    }

    const contexts = association.getPresentationContexts();
    contexts.forEach((c) => {
      const context = association.getPresentationContext(c.id);
      if (
        context.getAbstractSyntaxUid() === SopClass.Verification ||
        context.getAbstractSyntaxUid() === SopClass.StudyRootQueryRetrieveInformationModelFind ||
        context.getAbstractSyntaxUid() === SopClass.ModalityWorklistInformationModelFind ||
        Object.values(StorageClass).includes(context.getAbstractSyntaxUid())
      ) {
        const transferSyntaxes = context.getTransferSyntaxUids();
        transferSyntaxes.forEach((transferSyntax) => {
          if (
            transferSyntax === TransferSyntax.ImplicitVRLittleEndian ||
            transferSyntax === TransferSyntax.ExplicitVRLittleEndian
          ) {
            context.setResult(PresentationContextResult.Accept, transferSyntax);
          } else {
            context.setResult(PresentationContextResult.RejectTransferSyntaxesNotSupported);
          }
        });
      } else {
        context.setResult(PresentationContextResult.RejectAbstractSyntaxNotSupported);
      }
    });
    this.sendAssociationAccept();
  }

  cEchoRequest(
    request: requests.CEchoRequest,
    callback: (response: responses.CEchoResponse) => void
  ) {
    const response = CEchoResponse.fromRequest(request);
    response.setStatus(Status.Success);
    callback(response);
  }

  cFindRequest(
    request: requests.CFindRequest,
    callback: (responses: Array<responses.CFindResponse>) => void
  ) {
    console.log(request.getDataset());

    const response1 = CFindResponse.fromRequest(request);
    response1.setDataset(new Dataset({ PatientID: '12345', PatientName: 'JOHN^DOE' }));
    response1.setStatus(Status.Pending);

    const response2 = CFindResponse.fromRequest(request);
    response2.setStatus(Status.Success);

    callback([response1, response2]);
  }

  cStoreRequest(
    request: requests.CStoreRequest,
    callback: (response: responses.CStoreResponse) => void
  ) {
    console.log(request.getDataset());

    const response = CStoreResponse.fromRequest(request);
    response.setStatus(Status.Success);
    callback(response);
  }

  associationReleaseRequested() {
    this.sendAssociationReleaseResponse();
  }
}

const host = '127.0.0.1';
const port = 2104;
const callingAeTitle = 'SCU';
const calledAeTitle = 'ANY-SCP';

const server = new Server(ExampleScp);
server.listen(port);

const opts = {
  userIdentity: {
    type: UserIdentityType.UsernameAndPasscode,
    positiveResponseRequested: true,
    primaryField: 'Username',
    secondaryField: 'Password',
  },
};

const operations = [performCEcho, performCFindStudy, performCFindMwl, performCStore];
operations.forEach((o) => {
  Reflect.apply(o, null, [host, port, callingAeTitle, calledAeTitle, opts]);
});

setTimeout(() => {
  server.close();
  const statistics = server.getStatistics();
  console.log('Server statistics:', statistics.toString());
}, 3000);
