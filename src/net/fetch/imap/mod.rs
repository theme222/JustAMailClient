pub mod types;
pub use types::*;
use crate::*;

use std::error::Error;
use std::fmt;

use async_imap::extensions::idle::IdleResponse::{ManualInterrupt, NewData, Timeout};
use async_imap::types::{Seq, UnsolicitedResponse};
use async_native_tls::TlsStream;
use futures::{Stream, StreamExt, TryStreamExt};
use imap_proto::Response::MailboxData;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use bitflags::bitflags;
use async_imap::error::Error as AsyncImapError;
use async_imap::error::Result as AsyncImapResult;

pub struct ImapSession {
    pub id: ImapSessionId,
    pub capabilities: Option<CapabilitiesList>,
    pub net: Option<async_imap::Session<Compat<TlsStream<TcpStream>>>>,
    pub current_mailbox: Option<Mailbox>,
    pub receiver: tokio::sync::mpsc::Receiver<ImapSessionCommand>,
    pub abort: Aborter, // Instantly kill the current command and shutdown the session
}

impl ImapSession {
    // Imap session itself doesn't handle retries. The retry logic is handled by the manager.

    async fn get_client(
        cred: CredentialID,
    ) -> std::result::Result<(async_imap::Session<Compat<TlsStream<TcpStream>>>, Option<async_imap::types::Capabilities>), ImapSessionError>
    {
        let cred = CredentialStore::get(cred);

        // Assert right now because I have no other support for anything else
        assert!(cred.auth_method == AuthMethod::LOGIN);
        assert!(cred.encryption_method == EncryptionMethod::SSLTLS);

        let imap_addr = (cred.fetch_server.clone(), cred.fetch_port);
        let tcp_stream = TcpStream::connect(&imap_addr).await?;
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls
            .connect(cred.fetch_server.clone(), tcp_stream)
            .await?
            .compat();

        let mut client = async_imap::Client::new(tls_stream);
        Ok(client
            .login_with_capabilities(&cred.login, &cred.secret)
            .await
            .map_err(|e| e.0)?)
    }

    pub async fn setup(&mut self) -> ImapSessionResult<()> { 
        // Commands here aren't in get_client() since it may fail due to incompatible server capabilities
        self.enable(&caps![Capability::QRESYNC]).await?;
        Ok(())
    }

    pub async fn new(
        id: ImapSessionId,
        caps: Option<CapabilitiesList>,
    ) -> ImapSessionResult<
        (
            tokio::sync::mpsc::Sender<ImapSessionCommand>,
            tokio::sync::mpsc::Sender<()>,
        )
    > {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ImapSessionCommand>(100);
        let (abort_sender, abort_recv) = tokio::sync::mpsc::channel::<()>(5);
        let mut session = ImapSession {
            id: id.clone(),
            capabilities: caps,
            net: Some(Self::get_client(id.m_id).await?.0),
            current_mailbox: None,
            receiver: receiver,
            abort: abort_recv,
        };
        session.setup().await?;
        tokio::spawn(session.run());
        Ok((sender, abort_sender))
    }

    pub async fn run(mut self) {
        use FetchType::*;
        use ImapSessionCommandType::*;
        use NetAction::*;
        use SessionUpdate::*;

        Senders::net(NetMessage {
            action: IMAPUPDATE {
                cred_id: self.id.m_id,
                update: STARTED(self.id.clone()),
            },
            resolve: NULL_RESOLVE_ID,
        })
        .await;

        while let Some(command) = tokio::select!(
            command = self.receiver.recv() => command,
            command = wait_with_jitter(NETSOCK_REFRESH_INTERVAL) => Some(ImapSessionCommand { id: u64::MAX, ty: NOOP, resolve: NULL_RESOLVE_ID }),
            command = self.abort.recv() => Some(ImapSessionCommand { id: u64::MAX, ty: ImapSessionCommandType::SHUTDOWN, resolve: NULL_RESOLVE_ID }),
        ) {
            // println!("Imap Session running: {:?}", command);
            
            let res_id = command.resolve;
            // We will only resolve if the command succeeds or has an unrecoverable failure (so not retries)
            let res = match &command.ty {
                NOOP => self.noop(res_id).await,
                IDLE(mb) => self.idle(mb.into(), res_id).await,
                SELECT(mb) => self.select(res_id, &mb, true).await,
                NOOPON(mb) => self.noopon(res_id, &mb).await,
                ImapSessionCommandType::SHUTDOWN => {
                    ResolveStore::resolve(res_id, Resolution::Nothing);
                    break;
                },
                LISTFETCH(mb, seq_range) => {
                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &vec![
                            UID,
                            BODYSTRUCTURE,
                            ENVELOPE,
                            FLAGS,
                            INTERNALDATE,
                            RFC822SIZE,
                            BODYPEEKSECTION("TEXT".into(), format!("0.{}", MAX_LISTFETCH_SIZE)),
                            CHANGEDSINCE(1),
                            VANISHED
                        ],
                        command.ty.clone(),
                    )
                    .await
                }
                SECTIONFETCH(mb, seq_range, mail_body_structures) => {
                    let fetch_types: Vec<FetchType> = vec![
                        UID,
                        BODYSTRUCTURE,
                        ENVELOPE,
                        FLAGS,
                        INTERNALDATE,
                        RFC822SIZE,
                        BODYPEEKSECTION("HEADER".into(), "".into()),
                    ];

                    let fetch_body_sections: Vec<FetchType> = mail_body_structures
                        .iter()
                        .map(|mb| BODYPEEKSECTION(mb.part_spec_str(), "".into()))
                        .collect();

                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &fetch_types.into_iter().chain(fetch_body_sections).collect(),
                        command.ty.clone(),
                    )
                    .await
                }
                FULLFETCH(mb, seq_range, _) => {
                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &vec![
                            UID,
                            BODYSTRUCTURE,
                            ENVELOPE,
                            FLAGS,
                            INTERNALDATE,
                            RFC822SIZE,
                            BODYPEEKSECTION("HEADER".into(), "".into()),
                            BODYPEEKSECTION("TEXT".into(), "".into()),
                        ],
                        command.ty.clone(),
                    )
                    .await
                }
                SEARCH(_, items, seq_range) => self.search(res_id, items, seq_range).await,
            };
            

            if let Ok(success) = res {
                if command.id == u64::MAX { }
                else if success == ImapSessionSuccess::ONCE {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: CMDSUCCESS(self.id.clone(), command.id),
                            cred_id: self.id.m_id
                        },
                        resolve: NULL_RESOLVE_ID // Do not inherit cause we will have already resolved it
                    })
                    .await;
                }
                else if success == ImapSessionSuccess::REPEAT {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: CMDSUCCESSRETRY(self.id.clone(), command.id),
                            cred_id: self.id.m_id
                        },
                        resolve: res_id // Inherit resolve id
                    })
                    .await;
                }
            }
            else if let Err(err) = res {
                let net_exists = self.net.is_some();
                eprintln!("Err while executing command {:?}: {:?}", command.ty, err);
                use AsyncImapError::*;
                use ImapSessionError::*;
                let session_update = match &err {
                    ASYNCIMAPERROR(Io(error)) => { CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(Bad(_)) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(No(_)) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(ConnectionLost) => { CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(Parse(parse_error)) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(Validate(validate_error)) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(Append) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ASYNCIMAPERROR(_) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    TIMEOUT => CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists),
                    INVALIDRESPONSE(_) => { CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists) }
                    ABORTED => { self.net = None; CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists) }
                    TLSERROR(error) => unreachable!(), // Probably idk
                    AUTHENTICATIONERROR => unreachable!(), // I better hope so
                };

                if let CMDFAILURETRYAGAIN(_, _, _) = session_update {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: session_update,
                            cred_id: self.id.m_id,
                        },
                        resolve: res_id, // Inherit res_id
                    }).await;
                }
                else if let CMDFAILUREUNRECOVERABLE(_, _, _) = session_update {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: session_update,
                            cred_id: self.id.m_id,
                        },
                        resolve: NULL_RESOLVE_ID, // We will resolve this right now
                    }).await;
                    ResolveStore::fail(res_id, err.into());
                }


                // Check to see if we still own the network
                if self.net.is_none() { break; }
            }

            while !self.net.as_mut().unwrap().unsolicited_responses.is_empty() {
                let Ok(us) = self.net.as_mut().unwrap().unsolicited_responses.recv().await else { break; };
                println!("Received unsolicted response {:?}", us);
            }

        }

        self.net.take(); // force drop the network either way
        // This notifies the shutdown of the session (usually from aborts or the sender of the session gets dropped)
        Senders::net(NetMessage {
            action: IMAPUPDATE {
                update: SESSIONABORT(self.id.clone()),
                cred_id: self.id.m_id,
            },
            resolve: NULL_RESOLVE_ID
        })
        .await;
    }

    fn get_net(&mut self) -> &mut async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.as_mut().unwrap()
    }

    fn take_net(&mut self) -> async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.take().unwrap()
    }

    fn return_net(&mut self, net: async_imap::Session<Compat<TlsStream<TcpStream>>>) {
        self.net = Some(net);
    }

    fn net_abort(
        &mut self,
    ) -> (
        &mut async_imap::Session<Compat<TlsStream<TcpStream>>>,
        &mut tokio::sync::mpsc::Receiver<()>,
    ) {
        // If its stupid but it works, is it really stupid?
        let net = self.net.as_mut().unwrap();
        let abort = &mut self.abort;
        (net, abort)
    }

    pub async fn call_with_abort<F: Future<Output = AsyncImapResult<T>>, T>(
        abort: &mut tokio::sync::mpsc::Receiver<()>,
        future: F,
    ) -> ImapSessionResult<T> {
        let val = tokio::select! {
            _ = abort.recv() => { return Err(ImapSessionError::ABORTED); }
            _ = tokio::time::sleep(NETSOCK_TIMEOUT) => { return Err(ImapSessionError::TIMEOUT); }
            res = future => { res? }
        };
        Ok(val)
    }

    // This section onwards will contain wrappers to common imap protocol operations. 
    // If the function name starts with an _ it will return the direct result from the call.
    // Otherwise, it will simply return ImapSessionSuccess (This implies the result was already sent through channels or dropped)

    // NOOP: Do nothing and it won't fail (sometimes)
    pub async fn noop(&mut self, res_id: ResolveID) -> ImapSessionResult<ImapSessionSuccess> {
        let (network, abort) = self.net_abort();
        let res = Self::call_with_abort(abort, network.noop()).await;
        // let a = network.unsolicited_responses.try_recv();
        // println!("{:?}", a);
        ResolveStore::resolve(res_id, Resolution::Nothing);
        Ok(ImapSessionSuccess::ONCE)
    }

    // Call noop on a specific mailbox
    pub async fn noopon(&mut self, res_id: ResolveID, mb_name: &str) -> ImapSessionResult<ImapSessionSuccess> {
        self.select(res_id, mb_name, false).await?;
        self.noop(res_id).await
    }

    // SELECT: Select a mailbox on the server.
    pub async fn select<'a>(self: &'a mut Self, res_id: ResolveID, mb_name: &str, force: bool) -> ImapSessionResult<ImapSessionSuccess> {
        if let Some(curr_mb) = &self.current_mailbox {
            if curr_mb.name == mb_name && !force {
                ResolveStore::resolve(res_id, Resolution::Nothing);
                return Ok(ImapSessionSuccess::ONCE);
            }
        }
        let (network, abort) = self.net_abort();
        let mailbox = Self::call_with_abort(abort, network.select_condstore(mb_name)).await?;
        let mut mailbox: Mailbox = (&mailbox).into();
        mailbox.name = mb_name.into();
        let mut mailbox_sql: db::MailboxSQL = (&mailbox).into();  
        
        let acc_key: db::AccountKey = CredentialStore::get(self.id.m_id).into();
        mailbox_sql.id = Some(db::KeyWrapper(
            db::MailboxKey::ACCIDNAME( acc_key.clone(), mailbox_sql.name.clone().unwrap() )
        ));
        mailbox_sql.account_id = Some(db::KeyWrapper(acc_key));

        self.current_mailbox = Some(mailbox);
        self.current_mailbox
            .as_mut()
            .map(|mut mb| mb.name = mb_name.into());
        // println!("Selected mailbox: {:?}", self.current_mailbox);
        Senders::srv(SrvMessage {
            action: srv::SrvAction::SYNCMAILBOX {
                mb: mailbox_sql.clone(),
            },
            resolve: NULL_RESOLVE_ID
        })
        .await;
        ResolveStore::resolve(res_id, Resolution::MailboxSQL(mailbox_sql));
        Ok(ImapSessionSuccess::ONCE)
    }

    // FETCH: Fetch a stream of mails from the server.
    pub async fn fetch(
        self: &mut Self,
        res_id: ResolveID,
        mb: MailboxName,
        ss: &SeqRange,
        fetch_types: &Vec<FetchType>,
        from: ImapSessionCommandType,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        self.select(NULL_RESOLVE_ID, &mb, true).await?;
        let creds = self.id.m_id;
        let fetch_query = FetchType::fetch_string(fetch_types);
        let sss = ss.to_string();
        println!("Fetching: {} {}", sss, fetch_query);

        let (network, abort) = self.net_abort();
        
        let mut stream_normal: Option<_> = None;
        let mut stream_uid: Option<_> = None;
        
        // It aint the most elegant solution, but it works
        // I can't think of one right now so maybe later? 
        // (No putting it in a ternary doesn't work cause impls aren't the same type)
        if !ss.is_uid {
            stream_normal = Some(Self::call_with_abort(abort, 
                network.fetch(&sss, fetch_query)
            ).await?);
        } else {
            stream_uid = Some(Self::call_with_abort(abort, 
                network.uid_fetch(&sss, fetch_query)
            ).await?);
        }

        let mut err: Option<AsyncImapError> = None;
        let mut msgs: Vec<db::MessageSQL> = Vec::new();
        let mut msg_parts_sqls: Vec<db::MessagePartSQL> = Vec::new();

        while let Some(mail_result) = {
            let res = tokio::select! {
                _ = abort.recv() => { Err(ImapSessionError::ABORTED) }
                _ = tokio::time::sleep(NETSOCK_TIMEOUT) => { Err(ImapSessionError::TIMEOUT) }
                res = async {
                    if ss.is_uid { stream_uid.as_mut().unwrap().next().await }
                    else { stream_normal.as_mut().unwrap().next().await }
                } => { Ok(res) }
            };

            if let Err(e) = res {
                Some(Err(e))
            } else if let Ok(None) = res {
                None
            } else if let Ok(Some(Err(e))) = res {
                Some(Err(e.into()))
            } else if let Ok(Some(Ok(r))) = res {
                Some(Ok(r))
            } else {
                unreachable!()
            }
        } {
            if let Err(e) = mail_result {
                eprintln!("{:?}", e);
                use ImapSessionError::*;
                match e {
                    ASYNCIMAPERROR(e) => {
                        err = Some(e);
                        continue;
                    }
                    ABORTED | TIMEOUT => return Err(e),
                    _ => unreachable!(),
                }
            } 
            use SrvAction::*;
            use ImapSessionCommandType::*;
            use db::*;
            
            let mail = mail_result.unwrap();
            let message = Message::from(&mail);
            
            let mut msg_sql = db::MessageSQL::from(&message);
            let acc_key: db::AccountKey = CredentialStore::get(creds).into();
            let message_key = KeyWrapper( MessageKey::ACCIDIMAPUID( acc_key.clone(), msg_sql.imap_uid.unwrap() ) );
            msg_sql.id = Some(message_key.clone()); // Explicitly set the id because MessageSQL::from() simply doesn't have enough information to infer this
            msg_sql.account_id = Some(db::KeyWrapper( acc_key ));
            
            let mut msg_parts_sql: Vec<db::MessagePartSQL> = Vec::new();
            
            'match_from: {
                match &from {
                    LISTFETCH(_, seq_range) | FULLFETCH(_, seq_range, _) => {
                        if matches!(from, LISTFETCH(_, _)) && msg_sql.size.unwrap() > MAX_LISTFETCH_SIZE as i64 { break 'match_from; }
                        let body_opt = mail.text().and_then(|txt| mailparse::parse_mail(txt).ok());
                        if body_opt.is_none() || message.bodystructure.is_none() { break 'match_from; }
                        let body = body_opt.unwrap();
                        let bodystructure = message.bodystructure.as_ref().unwrap();
                        let mut parts = body.parts();
                        let mut dfs_traverse = parts.zip(bodystructure.clone().into_iter());
                        while let Some((mailparse_part, mailbodystructure)) = dfs_traverse.next() {
                            if !mailparse_part.subparts.is_empty() { continue; }
                            msg_parts_sql.push(
                                db::MessagePartSQL {
                                    id: Some(KeyWrapper(MessagePartKey::MSGIDPARTSPEC(message_key.clone().0, mailbodystructure.part_spec_str()))),
                                    create_time: None,
                                    update_time: None,
                                    message_id: Some(message_key.clone()),
                                    part_spec: Some(mailbodystructure.part_spec_str()),
                                    data: Some(mailparse_part.raw_bytes.to_vec()),
                                }
                            );
                        }
                    },
                    SECTIONFETCH(_, seq_range, mail_body_structures) => {
                        let mail_body_structures = mail_body_structures.clone();
                        let message_parts = mail_body_structures
                            .iter()
                            .map(|bs| 
                                db::MessagePartSQL {
                                    id: Some(KeyWrapper(MessagePartKey::MSGIDPARTSPEC(message_key.clone().0, bs.part_spec_str()))),
                                    create_time: None,
                                    update_time: None,
                                    message_id: Some(message_key.clone()),
                                    part_spec: Some(bs.part_spec_str()),
                                    data: mail.section(&imap_proto::SectionPath::Part(bs.part_spec().clone(), None)).map(Vec::from),
                                }
                            )
                            .filter(|mps| mps.data.is_some());
                        msg_parts_sql.extend(message_parts);
                    },

                    _ => unreachable!()
                }
            }

            
            Senders::srv(SrvMessage {
                action: SrvAction::SYNCEMAIL { msg: msg_sql.clone() },
                resolve: NULL_RESOLVE_ID
            }).await;

            for mps in &msg_parts_sql {
                Senders::srv(SrvMessage {
                    action: SrvAction::SYNCEMAILSECTION { part: mps.clone() },
                    resolve: NULL_RESOLVE_ID,
                }).await;
            }
            
            msgs.push(msg_sql);
            msg_parts_sqls.extend(msg_parts_sql);

        }

        ResolveStore::resolve(res_id, Resolution::MessageAndPartSQL(msgs, msg_parts_sqls));
        // drop(stream); // stream bounded to the network so we gotta drop it first
        if let Some(e) = err {
            return Err(ImapSessionError::ASYNCIMAPERROR(e));
        } // due to limitations, we simply just return the last one found
        Ok(ImapSessionSuccess::ONCE)
    }

    // DELETE: Mark mails as deleted and expunge them.
    pub async fn delete<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        self.store(mb, ss, '+', &vec![MailFlag::DELETED]).await?;
        let (network, abort) = self.net_abort();
        Self::call_with_abort(abort, network.expunge()).await?;
        Ok(ImapSessionSuccess::ONCE)
    }

    // STORE: Update flags of a mail.
    pub async fn store<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
        store_type: char,
        flags: &Vec<MailFlag>,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        if store_type != '+' && store_type != '-' {
            panic!("store_type must be '+' or '-'")
        }

        self.select(NULL_RESOLVE_ID, &mb, false).await?;
        let flag_string = MailFlag::flag_string(flags);
        let sss = ss.to_string();
        let (network, abort) = self.net_abort();
        let res = Self::call_with_abort(
            abort,
            network.store(
                sss,
                format!(
                    "{}FLAGS.SILENT {}", // Silent to avoid server echoing back the updated flags cause like why tho that kinda useless
                    store_type, flag_string
                ),
            ),
        )
        .await?;

        Ok(ImapSessionSuccess::ONCE)
    }

    // // APPEND: Append a mail to the mailbox.
    // pub async fn append<'a>(
    //     self: &'a mut Self,
    //     folder: &str,
    //     flags: &Vec<MailFlag>,
    //     date: Option<&str>,
    //     body: String,
    // ) -> ImapSessionResult<()> {
    //     let flag_string = MailFlag::flag_string(flags);
    //     let flags_arg = if flags.len() == 0 { None } else { Some(flag_string.as_str()) };
    //     let date_args = if date.is_none() { None } else { Some(date.unwrap()) };
    //     self.get_net()
    //         .append(folder, flags_arg, date_args, body.as_bytes())
    //         .await?;
    //     Ok(())
    // }

    // IDLE: Wait for new mails to arrive.
    pub async fn idle<'a>(&mut self, mb: MailboxName, res_id: ResolveID) -> ImapSessionResult<ImapSessionSuccess> {
        assert!(self.capabilities.expect("Capabilities list must exist").able_to(Capability::IDLE));
        self.select(NULL_RESOLVE_ID, &mb, false).await?;

        // From this point onwards if it fails the network connection will be unrecoverable and has to be remade.

        let mut handle = self.take_net().idle();
        let init_res = handle.init().await?;
        let (
            idle_wait_future,
            this_guy_has_to_have_a_name_otherwise_it_will_get_dropped_and_the_future_will_return_with_a_manual_interrupt_every_single_time,
        ) = handle.wait_with_timeout(with_jitter(NETSOCK_REFRESH_INTERVAL));
        let res = Self::call_with_abort(&mut self.abort, idle_wait_future).await?;
        println!("idle result: {:?}", res);
        let acc_key: db::AccountKey = CredentialStore::get(self.id.m_id).into();
        let mb_key = db::MailboxKey::ACCIDNAME(acc_key.clone(), mb.clone()); 
        let mut curr_mb = db::MailboxSQL::default();
        curr_mb.id = Some(db::KeyWrapper(mb_key.clone()));
        
        let res = match &res {
            ManualInterrupt => unreachable!(),
            Timeout =>  Ok(ImapSessionSuccess::REPEAT),
            NewData(response_data) => {
                use imap_proto::Response::*;
                match response_data.borrow_dependent() {
                    MailboxData(mailbox_datum) => {
                        use imap_proto::MailboxDatum::*;
                        match mailbox_datum {
                            Exists(e) => {
                                curr_mb.mail_count = Some(*e as i64);
                                Senders::srv(
                                    SrvMessage { 
                                        action: SrvAction::SYNCMAILBOX { mb: curr_mb }, 
                                        resolve: NULL_RESOLVE_ID 
                                    }
                                );
                            },
                            _ => return Err(ImapSessionError::INVALIDRESPONSE(format!("{:?}", response_data))),
                        }
                    },
                    Expunge(seqnum) => {
                        Senders::net( // Force refresh of the mailbox since this tells us literally nothing.
                            NetMessage { 
                                action: NetAction::SELECT { cred_id: self.id.m_id, mb } , 
                                resolve: NULL_RESOLVE_ID 
                            }
                        );
                    },
                    Fetch(seqnum, attribute_values) => {
                        let mut uid: Option<i64> = None;
                        let mut modseq: Option<i64> = None;
                        let mut flags: Option<Vec<String>> = None;

                        for attr in attribute_values {
                            use imap_proto::AttributeValue::*;
                            match attr {
                                Flags(cows) => flags = Some(cows.into_iter().map(|cow| cow.to_string()).collect()),
                                ModSeq(val) => modseq = Some(*val as i64),
                                Uid(val) => uid = Some(*val as i64),
                                _ => eprintln!("Received unknown attribute: {:?}", attr),
                            }
                        }
                        
                        if uid.is_none() { return Err(ImapSessionError::INVALIDRESPONSE(format!("No UID attribute received"))); }
                        let mut curr_msg = db::MessageSQL::default();
                        curr_msg.id = Some(db::KeyWrapper(db::MessageKey::ACCIDIMAPUID(acc_key, uid.unwrap())));
                        curr_msg.modseq = modseq;
                        curr_msg.flags = flags.and_then(|flags| serde_sqlite_jsonb::to_vec(&flags).ok());
                        Senders::srv( SrvMessage { action: SrvAction::SYNCEMAIL { msg: curr_msg }, resolve: NULL_RESOLVE_ID } );
                    }, 
                    Vanished { earlier, uids } => { // idk what earlier means
                        
                        Senders::net( // Force refresh the mailbox
                            NetMessage { 
                                action: NetAction::SELECT { cred_id: self.id.m_id, mb } , 
                                resolve: NULL_RESOLVE_ID 
                            }
                        );

                        // This isn't the smartest way to do this
                        uids.clone().into_iter().flatten().map(|uid| {
                            Senders::srv(
                                SrvMessage {
                                    action: SrvAction::RMMESSAGE { msg_k: db::MessageKey::ACCIDIMAPUID(acc_key.clone(), uid as i64) },
                                    resolve: NULL_RESOLVE_ID
                                }
                            )
                        });
                        
                    },
                    _ => return Err(ImapSessionError::INVALIDRESPONSE(format!("{:?}", response_data)))
                }
                Ok(ImapSessionSuccess::REPEAT)
            }
        };

        let network = Self::call_with_abort(&mut self.abort, handle.done()).await?;
        self.return_net(network);
        res
    }

    // ENABLE: Enable a set of capabilities
    pub async fn enable(&mut self, capabilities: &CapabilitiesList) -> ImapSessionResult<ImapSessionSuccess> {
        use Capability::*;
        let cap_str = capabilities.to_string();
        let (net, abort) = self.net_abort();
        Self::call_with_abort(abort, net.run_command_and_check_ok(format!("ENABLE {}", cap_str))).await?;
        Ok(ImapSessionSuccess::ONCE)
    }

    // CAPABILITIES: Get the server's capabilities
    pub async fn _capabilities(&mut self) -> ImapSessionResult<CapabilitiesList> {
        let (net, abort) = self.net_abort();
        let capabilities = Self::call_with_abort(abort, net.capabilities()).await?;
        Ok(capabilities.into())
    }

    // LIST: Get the list of mailboxes
    pub async fn _list(&mut self) -> ImapSessionResult<impl Stream<Item = AsyncImapResult<async_imap::types::Name>>> {
        let (net, abort) = self.net_abort();
        let list = Self::call_with_abort(abort, net.list(None, Some("*"))).await?;
        Ok(list)
    }
    
    // SEARCH: Search for messages and return the matching sequence numbers / uids
    pub async fn _search(&mut self, search_query: &Vec<SearchQuery>, ss: &SeqRange) -> ImapSessionResult<std::collections::HashSet<u32>> {
        let (net, abort) = self.net_abort();
        let query = SearchQuery::to_query_str(search_query);
        let query = if ss.is_uid { format!("UID {} {}", ss, query) } else { format!("{} {}", query, ss) };
        let found = if ss.is_uid { Self::call_with_abort(abort, net.uid_search(query)).await? }
                    else { Self::call_with_abort(abort, net.search(query)).await? };
        Ok(found)
    }

    pub async fn search(&mut self, res_id: ResolveID, search_query: &Vec<SearchQuery>, ss: &SeqRange) -> ImapSessionResult<ImapSessionSuccess> {
        let vals = self._search(search_query, ss).await?;
        ResolveStore::resolve(res_id, Resolution::Search(vals));
        Ok(ImapSessionSuccess::ONCE)
    }
}

#[derive(Clone, Debug)]
pub struct ImapSessionState {
    // Since during creation of ImapSession we do not actually own it we must keep track of its state based on what we send and what the responses are.
    pub to_session: Option<tokio::sync::mpsc::Sender<ImapSessionCommand>>,
    pub to_abort: Option<tokio::sync::mpsc::Sender<()>>,
    pub pending_cmds: std::collections::VecDeque<ImapSessionCommand>, // With the current implementations this actually doesn't need to be a VecDeque haha
    pub last_known_mailbox: Option<MailboxName>,
    running: bool,
    failed: bool,
}

pub fn imap_connection_retry_logic<T>(res: &ImapSessionResult<T>) -> Attempt {
    use ImapSessionError::*;
    use async_imap::error::Error::*;
    match res {
        Ok(_) => Attempt::ONCE,
        Err(ASYNCIMAPERROR(Io(_)) | ASYNCIMAPERROR(ConnectionLost)) => Attempt::REPEAT,
        _ => Attempt::ONCE,
    }
}

impl ImapSessionState {
    pub async fn get_imap_session(
        id: ImapSessionId,
        caps: CapabilitiesList,
    ) -> Option<(
        tokio::sync::mpsc::Sender<ImapSessionCommand>,
        tokio::sync::mpsc::Sender<()>,
    )> {
        use ImapSessionError::*;
        use async_imap::error::Error::*;
        let mut current_retry_delay = INITIAL_RETRY_DELAY;
        loop {
            let res = ImapSession::new(id.clone(), Some(caps)).await;
            if let Err(e) = &res { eprintln!("{e}"); }
            if imap_connection_retry_logic(&res) == Attempt::ONCE { return res.ok(); }
            wait_with_jitter(current_retry_delay).await;
            current_retry_delay = double_time_clamped(current_retry_delay);
        }
        return None;
    }

    pub fn new(id: ImapSessionId) -> Self {
        Self {
            to_session: None,
            to_abort: None,
            pending_cmds: std::collections::VecDeque::new(),
            last_known_mailbox: None,
            running: false,
            failed: false,
        }
    }

    pub async fn connect(&mut self, id: ImapSessionId, caps: CapabilitiesList) -> bool {
        // Do not run this if we are already connencted
        let res = Self::get_imap_session(id, caps).await;
        let success = res.is_some();
        self.running = false;
        self.failed = false;
        if let Some((to_session, to_abort)) = res {
            self.to_session = Some(to_session);
            self.to_abort = Some(to_abort);

            // Send any pending commands back to the session
            for cmd in self.pending_cmds.iter() {
                self.to_session
                    .as_ref()
                    .unwrap()
                    .send(cmd.clone())
                    .await
                    .unwrap();
            }
        }
        success
    }

    pub async fn add_command(&mut self, cmd: ImapSessionCommand) {
        let next_known_mailbox = cmd.get_mailbox_domain();
        if next_known_mailbox.is_some() {
            self.last_known_mailbox = next_known_mailbox.map(|mb| mb.to_string());
        }
        self.pending_cmds.push_back(cmd.clone());
        if let Some(sender) = self.to_session.as_ref() {
            sender.send(cmd).await.unwrap();
        }
    }

    pub fn rm_command(&mut self, id: u64) -> Option<ImapSessionCommand> {
        // id here either comes from the ImapSession calling itself or the ImapManager
        // where the imap session will always pick a command id of u64::MAX
        // This means that the id must be in the front of the queue or not a valid id that we send
        self.pending_cmds.pop_front_if(|cmd| cmd.id == id)
    }

    pub fn sum_weights(&self) -> f64 {
        self.pending_cmds.iter().map(|cmd| cmd.weight()).sum()
    }

    pub fn status(&self) -> Status {
        if self.failed {
            Status::FAILED
        } else if !self.running {
            Status::CONNECTING
        } else if self.pending_cmds.is_empty() {
            Status::ALIVE
        } else {
            Status::BUSY
        }
    }
}

const MINIMUM_CAPS_ALLOWED: CapabilitiesList = caps!( Capability::IMAP4rev1 );
const MINIMUM_CAPS_IDLE: CapabilitiesList = caps!( Capability::IMAP4rev1, Capability::IDLE );
static IDLE_IS_BROKEN: [Service; 2] = [Service::YAHOO, Service::AOL]; // Yahoo why you do this :(

pub struct ImapManager {
    // One manager per account
    pub id: CredentialID,
    pub imap_session_states: std::collections::HashMap<ImapSessionId, ImapSessionState>, // Key is id
    pub capabilities: CapabilitiesList,
    pub poll_strategy: PollStrategyMap,
}

impl ImapManager {

    // Evaluates the current state of the 
    fn evaluate_poll_strategy(&mut self) -> Result<()> { // If this fails we can't continue
        let active_sessions = self.get_active_session_count();
        let mailbox_names = self.poll_strategy.keys().cloned().collect::<Vec<_>>();
        let creds = CredentialStore::get(self.id);
        self.poll_strategy.values_mut().for_each(|s| { *s = PollingStrategy::SELECTSPAM; });
        println!("{:?}", self.capabilities.to_vec());
        if !self.capabilities.capable_to(MINIMUM_CAPS_ALLOWED) {
            return Err(ImapSessionError::INVALIDRESPONSE("Server does not support minimum required capabilities".to_string()).into());
        } else if active_sessions == 0 {
            return Err(anyhow::anyhow!("Failed to create any IMAP sessions"));
        } else if active_sessions >= 2 && self.capabilities.capable_to(MINIMUM_CAPS_IDLE) && !IDLE_IS_BROKEN.contains(&creds.service) {
            self.poll_strategy.insert("INBOX".into(), PollingStrategy::IDLE);
        }
        Ok(())
    }

    pub async fn new(m_id: CredentialID) -> Result<Self> {
        let (cap_list, poll_strat_map) = Self::probe_server(m_id).await?;
        let mut manager = Self {
            id: m_id,
            imap_session_states: std::collections::HashMap::new(),
            capabilities: cap_list,
            poll_strategy: poll_strat_map,
        };

        manager.create_session_states(m_id, MAX_IMAP_SESSIONS).await;
        manager.evaluate_poll_strategy()?;

        for (mb, poll_strat) in manager.poll_strategy.clone().into_iter() {
            if poll_strat != PollingStrategy::IDLE { continue; }
            manager
                .call_session(ImapSessionCommandType::IDLE(mb), NULL_RESOLVE_ID)
                .await;
        }
        Ok(manager)
    }

    pub async fn probe_server(m_id: CredentialID) -> ImapSessionResult<(CapabilitiesList, PollStrategyMap)> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ImapSessionCommand>(100);
        let (abort_sender, abort_recv) = tokio::sync::mpsc::channel::<()>(5);
        let mut client: Option<async_imap::Session<Compat<TlsStream<TcpStream>>>> = None;
        let mut cap_option: Option<async_imap::types::Capabilities> = None;
        let mut poll_strat_map: PollStrategyMap = std::collections::HashMap::new();
        
        loop {
            let res = ImapSession::get_client(m_id).await;
            if let Err(e) = &res { eprintln!("{e}"); }
            if imap_connection_retry_logic(&res) == Attempt::ONCE { 
                let (a, b) = res?; 
                client = Some(a);
                cap_option = b;
                break;
            }
        }
        
        let mut session = ImapSession {
            id: ImapSessionId { s_id: IDStore::s_id(), m_id },
            capabilities: None,
            net: client,
            current_mailbox: None,
            receiver: receiver,
            abort: abort_recv,
        };

        let cap_list = if cap_option.is_none() {
            session._capabilities().await?
        } else {
            cap_option.unwrap().into()
        };

        let mut stream = session._list().await?;
        while let Some(res) = stream.next().await {
            let Ok(name) = res else { continue };
            let attrs = name.attributes();
            let mb_name = name.name();
            println!("{:?} {}", attrs, mb_name);

            poll_strat_map.insert(mb_name.into(), PollingStrategy::SELECTSPAM);
            let acc_id: db::AccountKey = CredentialStore::get(m_id).into();
            let mailbox_sql = db::MailboxSQL {
                id: Some(db::KeyWrapper(db::MailboxKey::ACCIDNAME(
                    acc_id.clone(),
                    mb_name.to_string(),
                ))), // Personally I like the way I structured it.
                create_time: None,
                update_time: None,
                account_id: Some(db::KeyWrapper(acc_id.clone())),
                name: Some(mb_name.into()),
                mail_count: None,
                recent: None,
                unseen: None,
                attrs: Some(
                    serde_sqlite_jsonb::to_vec(
                        &attrs.iter().map(MailboxAttr::from).collect::<Vec<_>>()
                    ).unwrap()
                ),
                highest_modseq: None,
                uid_next: None,
                uid_validity: None,
            };
            Senders::srv(SrvMessage { action: SrvAction::SYNCMAILBOX { mb: mailbox_sql }, resolve: NULL_RESOLVE_ID }).await;
        }

        Ok((cap_list, poll_strat_map))
    }

    pub fn get_active_session_count(&self) -> usize {
        self.imap_session_states.iter().fold(
            0,
            |acc, (_, s)| {
                if s.to_session.is_some() { acc + 1 } else { acc }
            },
        )
    }

    pub fn status(&self) -> Vec<(ImapSessionId, Status)> {
        self.imap_session_states
            .clone()
            .into_iter()
            .map(|(id, s)| (id, s.status()))
            .collect()
    }

    pub async fn rcv_session_update(&mut self, upd: SessionUpdate, res_id: ResolveID) {
        use SessionUpdate::*;
        match upd {
            STARTED(isid) => {
                self.imap_session_states.get_mut(&isid).unwrap().running = true;
            }
            CMDSUCCESS(isid, cmdid) => {
                self.imap_session_states
                    .get_mut(&isid)
                    .unwrap()
                    .rm_command(cmdid);
            }
            CMDSUCCESSRETRY(isid, cmdid) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();
                let cmd = state.rm_command(cmdid).unwrap();
                self.call_session(cmd.ty, res_id).await;
            }
            SESSIONABORT(isid) => {
                // let state = self.imap_session_states.get_mut(&isid).unwrap();
                // state.running = false;
            }
            CMDFAILURETRYAGAIN(isid, cmdid, net_exists) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();

                if !net_exists { // Set failure
                    state.running = false;
                    state.failed = true;
                }

                let cmd = state.rm_command(cmdid).unwrap(); // rm previous command
                self.call_session(cmd.ty, res_id).await; // call it again (on hopefully one that didn't fail)

                if !net_exists { // Try to reconnect
                    let state = self.imap_session_states.get_mut(&isid).unwrap(); // Hello future me. This line is required. Sincerely, me.
                    state.connect(isid, self.capabilities).await;
                }
            }
            CMDFAILUREUNRECOVERABLE(isid, cmdid, net_exists) => {
                println!("Received unrecoverable failure for command {:?}", cmdid);
                let state = self.imap_session_states.get_mut(&isid).unwrap();

                if !net_exists {
                    state.running = false;
                    state.failed = true;
                    state.connect(isid, self.capabilities).await;
                }
            }
        }
    }

    pub async fn create_session_states(&mut self, m_id: CredentialID, count: usize) {
        // Try to create as many imap sessions per manager as possible
        use AsyncImapError::*;
        use ImapSessionError::*;
        use ResponseCode::*;

        for i in 0..count {
            let isid = ImapSessionId {
                s_id: IDStore::s_id(),
                m_id: m_id,
            };
            let mut state = ImapSessionState::new(isid.clone());
            if !state.connect(isid.clone(), self.capabilities).await {
                break;
            }
            self.imap_session_states.insert(isid, state);
        }
    }

    pub async fn call_session(&mut self, cmd: ImapSessionCommandType, resolve: ResolveID) {
        // Find the most available session to send the command to
        fn weight_by_diff_mailbox(iss: &ImapSessionState, cmd: &ImapSessionCommandType) -> f64 {
            (!cmd.get_required_mailbox().is_none()
                && iss.last_known_mailbox != cmd.get_required_mailbox().cloned()) as i64
                as f64
        }

        let lowest_weight_state = self
            .imap_session_states
            .values_mut()
            .into_iter()
            .filter(|state| !state.failed)
            .min_by(|a, b| {
                (a.sum_weights() + weight_by_diff_mailbox(a, &cmd))
                    .total_cmp(&(b.sum_weights() + weight_by_diff_mailbox(b, &cmd)))
            })
            .expect("No available sessions");

        lowest_weight_state
            .add_command(ImapSessionCommand {
                id: IDStore::cmd_id(),
                ty: cmd,
                resolve
            })
            .await;
    }

    pub async fn handle_suggest(&mut self, mb: MailboxName, res_id: ResolveID) {
        let available_session = self
            .imap_session_states
            .iter_mut()
            .filter(|(_, s)| s.pending_cmds.is_empty() && s.running && !s.failed)
            .next();
        if let Some((_, state)) = available_session {
            state
                .add_command(ImapSessionCommand {
                    id: IDStore::cmd_id(),
                    ty: ImapSessionCommandType::SELECT(mb),
                    resolve: res_id,
                })
                .await;
        }
    }

    pub async fn handle_poll(&mut self) {
        println!("Polling strategy: {:?}", self.poll_strategy);
        for (mb, poll_strat) in self.poll_strategy.clone().into_iter() {
            if poll_strat == PollingStrategy::IDLE {continue;}
            else if poll_strat == PollingStrategy::NOOPSPAM { unimplemented!("bruh") }
            self.call_session(ImapSessionCommandType::SELECT(mb), NULL_RESOLVE_ID).await;
        }
    }

    pub async fn shutdown(&mut self, res_id: ResolveID) {
        for (_, state) in self.imap_session_states.iter_mut() {
            state
                .add_command(ImapSessionCommand {
                    id: IDStore::cmd_id(),
                    ty: ImapSessionCommandType::SHUTDOWN,
                    resolve: res_id,
                })
                .await;
        }
    }
}