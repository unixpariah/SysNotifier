mod notify;
mod pulse;
mod upower;

use libpulse_binding::context::subscribe::{Facility, InterestMaskSet};
use notify::notify;
use pulse::Volume;
use std::collections::HashMap;
use std::sync::mpsc;
use upower::{BatteryLevel, BatteryManager, BatteryState};

struct SysNotifier<'a> {
    pulse: pulse::PulseManager,
    notifier: Notifier<'a>,
    event_channel: mpsc::Receiver<Event>,
    last_volume: Option<Volume>,
    battery: BatteryManager,
}

impl SysNotifier<'_> {
    async fn new() -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel();

        let mut battery = BatteryManager::new().await?;
        battery.subscribe(tx.clone()).await?;

        let mut pulse = pulse::PulseManager::new()?;
        pulse.set_subscription_callback(move |facility, _, _| {
            let event = match facility {
                Some(Facility::Sink) => Some(Event::VolumeChanged),
                Some(Facility::Card) => Some(Event::DefaultDeviceChanged),
                _ => None,
            };

            if let Some(event) = event {
                _ = tx.send(event);
            }
        });

        pulse.subscribe(
            InterestMaskSet::SERVER | InterestMaskSet::SINK_INPUT | InterestMaskSet::SINK,
        );

        Ok(Self {
            battery,
            event_channel: rx,
            pulse,
            notifier: Notifier::new().await?,
            last_volume: None,
        })
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            match self.event_channel.recv() {
                Ok(Event::VolumeChanged) => {
                    let volume = self.pulse.get_default_sink_volume()?;
                    if self
                        .last_volume
                        .as_ref()
                        .is_none_or(|last_volume| *last_volume != volume)
                    {
                        self.notifier.send_volume_notification(&volume).await?;
                        self.last_volume = Some(volume);
                    }
                }
                Ok(Event::DefaultDeviceChanged) => {
                    self.notifier.send_device_change_notification().await?;
                }
                Ok(Event::BatteryLevel(level)) => {
                    let percentage = self.battery.get_battery();
                    self.notifier
                        .send_battery_level_notification(percentage, &level)
                        .await?;
                }
                Ok(Event::BatteryState(state)) => {
                    let percentage = self.battery.get_battery();
                    self.notifier
                        .send_battery_state_notification(percentage, &state)
                        .await?;
                }
                Ok(Event::OnBattery(on_battery)) => {
                    let percentage = self.battery.get_battery();
                    self.notifier
                        .send_power_source_notification(percentage, on_battery)
                        .await?;
                }
                Ok(Event::BatteryPercentage(percentage)) => {
                    self.battery.set_battery(percentage);
                }
                Err(_) => break,
            }
        }
        Ok(())
    }
}

struct Notifier<'a> {
    builder: notify::NotificationBuilder<'a>,
    active_notifications: HashMap<Event, u32>,
}

impl<'a> Notifier<'a> {
    async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            builder: notify().await?,
            active_notifications: HashMap::new(),
        })
    }

    async fn send_volume_notification(&mut self, volume: &Volume) -> anyhow::Result<()> {
        let id = *self
            .active_notifications
            .get(&Event::VolumeChanged)
            .unwrap_or(&0);

        let mut builder = self.builder.clone().with_id(id);
        let volume_summary = format!("Volume [ {}% ]", volume.value);

        let icon_name = if volume.muted || volume.value == 0 {
            "audio-volume-muted-symbolic"
        } else if volume.value < 33 {
            "audio-volume-low-symbolic"
        } else if volume.value < 66 {
            "audio-volume-medium-symbolic"
        } else {
            "audio-volume-high-symbolic"
        };

        if volume.muted {
            builder = builder.with_summary("Volume Muted").with_icon(icon_name);
        } else {
            builder = builder
                .with_summary(&volume_summary)
                .with_progress(volume.value as i32)
                .with_icon(icon_name);
        }

        let new_id = builder.send().await?;
        self.active_notifications
            .insert(Event::VolumeChanged, new_id);

        Ok(())
    }

    async fn send_device_change_notification(&mut self) -> anyhow::Result<()> {
        let id = self
            .active_notifications
            .get(&Event::DefaultDeviceChanged)
            .unwrap_or(&0);
        let new_id = self
            .builder
            .clone()
            .with_urgency(notify::Urgency::Normal)
            .with_summary("Device changed")
            .with_id(*id)
            .send()
            .await?;

        self.active_notifications
            .insert(Event::DefaultDeviceChanged, new_id);
        Ok(())
    }

    async fn send_battery_state_notification(
        &mut self,
        percentage: u64,
        state: &BatteryState,
    ) -> anyhow::Result<()> {
        let id = *self
            .active_notifications
            .get(&Event::BatteryState(*state))
            .unwrap_or(&0);

        let (summary, icon) = match state {
            BatteryState::Charging => ("Battery is charging", "battery-charging-symbolic"),
            BatteryState::Empty => ("Battery is empty", "battery-empty-symbolic"),
            BatteryState::FullyCharged => {
                ("Battery fully charged", "battery-full-charged-symbolic")
            }
            _ => return Ok(()),
        };

        let new_id = self
            .builder
            .clone()
            .with_summary(summary)
            .with_icon(icon)
            .with_body(&format!("Current battery percentage: {percentage}%"))
            .with_progress(percentage as i32)
            .with_id(id)
            .send()
            .await?;

        self.active_notifications
            .insert(Event::BatteryState(*state), new_id);
        Ok(())
    }

    async fn send_battery_level_notification(
        &mut self,
        percentage: u64,
        level: &BatteryLevel,
    ) -> anyhow::Result<()> {
        let id = *self
            .active_notifications
            .get(&Event::BatteryLevel(*level))
            .unwrap_or(&0);

        let (summary, icon, urgency) = match level {
            BatteryLevel::Critical => (
                "Battery level critical",
                "battery-caution-symbolic",
                notify::Urgency::Critical,
            ),
            BatteryLevel::Low => (
                "Battery level low",
                "battery-low-symbolic",
                notify::Urgency::Normal,
            ),
            BatteryLevel::Normal => (
                "Battery level normal",
                "battery-good-symbolic",
                notify::Urgency::Low,
            ),
            BatteryLevel::High => (
                "Battery level high",
                "battery-full-symbolic",
                notify::Urgency::Low,
            ),
            BatteryLevel::Full => (
                "Battery level full",
                "battery-full-charged-symbolic",
                notify::Urgency::Low,
            ),
            _ => return Ok(()),
        };

        let new_id = self
            .builder
            .clone()
            .with_summary(summary)
            .with_icon(icon)
            .with_urgency(urgency)
            .with_body(&format!("Current battery percentage: {percentage}%"))
            .with_progress(percentage as i32)
            .with_id(id)
            .send()
            .await?;

        self.active_notifications
            .insert(Event::BatteryLevel(*level), new_id);
        Ok(())
    }

    async fn send_power_source_notification(
        &mut self,
        percentage: u64,
        on_battery: bool,
    ) -> anyhow::Result<()> {
        let id = *self
            .active_notifications
            .get(&Event::OnBattery(on_battery))
            .unwrap_or(&0);

        let (summary, icon) = if on_battery {
            ("Running on battery power", "battery-symbolic")
        } else {
            ("Connected to power", "ac-adapter-symbolic")
        };

        let new_id = self
            .builder
            .clone()
            .with_summary(summary)
            .with_body(&format!("Current battery percentage: {percentage}%"))
            .with_progress(percentage as i32)
            .with_icon(icon)
            .with_id(id)
            .send()
            .await?;

        self.active_notifications
            .insert(Event::OnBattery(on_battery), new_id);
        Ok(())
    }
}

#[derive(PartialEq, Eq, Hash)]
enum Event {
    VolumeChanged,
    DefaultDeviceChanged,
    BatteryState(BatteryState),
    BatteryLevel(BatteryLevel),
    OnBattery(bool),
    BatteryPercentage(u64),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sysnotifier = SysNotifier::new().await?;
    sysnotifier.run().await?;

    Ok(())
}
