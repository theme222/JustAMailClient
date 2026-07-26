// Will absolutely crash if not initialized before use
// These values must be set ONCE at the start of the program (before parallelizations occur) and stay constant
// Like yes ik I should do Arc Mutex whatever but ehhhhhhh can't be bothered
    
static mut SENDERS: Option<Senders> = None;

#[derive(Clone, Debug)]
pub struct Senders { // Global values after initialization
    pub net_sender: tokio::sync::mpsc::Sender<super::NetMessage>,
    pub srv_sender: tokio::sync::mpsc::Sender<super::SrvMessage>,
    pub gui_sender:  tokio::sync::mpsc::Sender<super::GUIMessage>,
}

#[allow(static_mut_refs)]
impl Senders {
    pub fn get() -> Self { unsafe { SENDERS.clone().expect("SENDERS has not been initialized") } }
    pub fn set(senders: Self) { 
        unsafe { 
            if SENDERS.is_some() { panic!("SENDERS is already initialized, cannot overwrite"); }
            SENDERS = Some(senders);
        };
    }

    pub async fn net(msg: super::NetMessage) { 
        if let Err(e) = Self::get().net_sender.send(msg).await {
            eprintln!("Failed to send NET message: {:?}", e);
        }
    }
    pub async fn srv(msg: super::SrvMessage) { 
        if let Err(e) = Self::get().srv_sender.send(msg).await {
            eprintln!("Failed to send SRV message: {:?}", e);
        }
    }
    pub async fn gui(msg: super::GUIMessage) { 
        if let Err(e) = Self::get().gui_sender.send(msg).await {
            eprintln!("Failed to send GUI message: {:?}", e);
        }
    }
}