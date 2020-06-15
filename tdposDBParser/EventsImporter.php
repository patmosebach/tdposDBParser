<?php
/*
 * BrokerVisions Tickets System
 *
 * @author    ARTIO http://www.artio.net
 * @copyright Copyright (C) 2011 ARTIO s.r.o.
 */

namespace Localtickets\Integration\Ticketmaster\Sync;

use JDatabaseDriver;
use JFactory;
use JTable;
use Localtickets\Enums\Marketplaces;
use Localtickets\Enums\Shippings;
use Localtickets\Enums\TicketSplit;
use Localtickets\Enums\TicketStatus;
use Localtickets\Integration\Ticketmaster\ProductionsMapper;
use Localtickets\Integration\Ticketmaster\Sync;
use LocalticketsModelBackground_process;
use LocalticketsModelEticket;
use LocalticketsModelEticketcollection;
use LocalticketsModelOrder2;
use LocalticketsModelOrder2_event_info;
use LocalticketsModelOrder2_item;
use LocalticketsModelOrder2_note;
use LocalticketsModelOrder2_shipping;
use LocalticketsModelTicket;
use LocalticketsModelTicketcollection;
use LocalticketsModelTmsync_account;
use LocalticketsModelTmsync_event;
use LocalticketsModelTmsync_history;
use LocalticketsModelTmsync_history_event;
use LocalticketsModelTmsync_history_item;
use LocalticketsModelTmsync_item;
use stdClass;
use TableEvent;
use TableTicket;
use TableTmsync_synced_event;
use TicketModel;
use TicketObject;
use TicketsCoreHelper;
use TicketsEventInventory;
use TicketsHelper;
use TicketsLogger;
use TicketsSeatsHelper;
use ticketsUploader;
use ticketsUploaderTicketmaster;

defined('_JEXEC') or die('Restrict Access');

/**
 * Imports SyncEvents from EI/TM, either pre-downloaded selected events or events directly downloaded from EI/TM
 */
class EventsImporter extends TicketObject
{
    /**
     * Current broker ID
     *
     * @var int
     */
    protected $brokerId;

    /**
     * Current user ID
     *
     * @var int
     */
    protected $userId;

    /**
     * Current download or import process
     *
     * @var LocalticketsModelBackground_process
     */
    protected $process = null;

    /**
     * Cached EI API instance
     *
     * @var TicketsEventInventory
     */
    protected $api = null;

    /**
     * Active EI uploader for current broker
     *
     * @var ticketsUploaderTicketmaster
     */
    protected $uploader = null;

    /**
     * Used to map EI production to our local event
     *
     * @var ProductionsMapper
     */
    protected $mapper = null;

    /**
     * Cached Sync Events Downloader
     *
     * @var SyncEventsDownloader
     */
    protected $downloader = null;

    /**
     * DB connector
     *
     * @var JDatabaseDriver
     */
    protected $db = null;

    /**
     * Current TM Sync history record
     *
     * @var LocalticketsModelTmsync_history
     */
    protected $history = null;

    /**
     * If specified, overrides account's "Match Only" setting
     *
     * @var bool|null
     */
    protected $matchOnlyOverride = null;

    /**
     * Initializes importer
     *
     * @param int $brokerId
     * @param int $userId
     * @param LocalticketsModelBackground_process $process
     */
    public function __construct($brokerId, $userId, $process = null)
    {
        parent::__construct();

        $this->brokerId = $brokerId;
        $this->userId = $userId;
        $this->process = $process;
        $this->db = JFactory::getDbo();
    }

    /**
     * Sets value to override account's "Match Only" setting
     *
     * @param bool|null $state
     */
    public function setMatchOnlyOverride(?bool $state)
    {
        $this->matchOnlyOverride = $state;
    }

    /**
     * Determines whether new inventory should be created automatically
     *
     * @param LocalticketsModelTmsync_account $account
     * @return bool
     */
    protected function isMatchOnly(LocalticketsModelTmsync_account $account): bool
    {
        // Use override if specified
        if (!is_null($this->matchOnlyOverride))
            return $this->matchOnlyOverride;

        return $account->isMatchOnly();
    }

    /**
     * Notifies current download process about update
     */
    protected function notifyProcess()
    {
        if ($this->process) {
            $this->process->notifyUpdate(true);
        }
    }

    /**
     * Returns cached instance of EI API connector
     *
     * @return TicketsEventInventory|bool
     */
    protected function getApi()
    {
        if (is_null($this->api)) {
            // Get TM uploader used for current broker
            $sync = Sync::getInstance();
            $uploader = $sync->getTmUploader($this->brokerId);
            if (!$uploader) {
                $this->api = false;
                return $this->setError($sync->getError());
            }

            $this->api = new TicketsEventInventory($uploader->get('broker_id'));
        }

        return $this->api;
    }

    /**
     * Returns active TM uploader instance for current broker
     *
     * @return ticketsUploaderTicketmaster
     */
    protected function getTmUploader()
    {
        if (is_null($this->uploader)) {
            $sync = Sync::getInstance();
            $uploader = $sync->getTmUploader($this->brokerId);

            if ($uploader && $uploader->isEnabled()) {
                // Found uploader, create instance
                $this->uploader = ticketsUploader::getInstance($uploader, $this->process);
                if (!is_object($this->uploader)) {
                    $this->uploader = false;
                }
            }
            else {
                // Active uploader not found
                $this->uploader = false;
            }
        }

        return $this->uploader;
    }

    /**
     * Returns cached instance of ProductionsMapper
     *
     * @return ProductionsMapper|bool
     */
    protected function getMapper()
    {
        if (!$this->mapper) {
            $api = $this->getApi();
            if (!$api)
                return false;

            $this->mapper = new ProductionsMapper($api);
        }

        return $this->mapper;
    }

    /**
     * Returns cached instance of SyncEventsDownloader
     *
     * @return SyncEventsDownloader
     */
    protected function getDownloader()
    {
        if (!$this->downloader) {
            $this->downloader = new SyncEventsDownloader($this->brokerId, $this->userId);
        }

        return $this->downloader;
    }

    /**
     * Returns cached instance of specified TM Sync account
     *
     * @param int $accountId
     * @return LocalticketsModelTmsync_account
     */
    protected function getAccount($accountId)
    {
        static $cache = [];

        if (!isset($cache[$accountId])) {
            $account = TicketModel::getModelInstance('tmsync_account');
            if (!$account->load($accountId)) {
                $cache[$accountId] = false;
            }
            else {
                $cache[$accountId] = $account;
            }
        }

        return $cache[$accountId];
    }

    /**
     * Imports selected sync events
     *
     * @param int[] $eventIds
     * @return bool
     */
    public function importSelectedEvents($eventIds)
    {
        // Remember list of account IDs affected
        $accountIds = [];

        // Import start
        $startTime = gmdate('Y-m-d H:i:s');

        // Cached history records by account ID
        $histories = [];

        // Loop through selected events and import them
        foreach ($eventIds as $eventId) {
            // Notify process
            $this->notifyProcess();

            // Load stored sync event
            /** @var LocalticketsModelTmsync_event $event */
            $event = TicketModel::getModelInstance('tmsync_event');
            if (!$event->load($eventId)) {
                continue;
            }

            // Skip if it doesn't belong to current broker and user
            if ($event->get('broker_id') != $this->brokerId ||
                $event->get('user_id') != $this->userId)
            {
                continue;
            }

            // #27210: Skip if sync is disabled
            if ($event->isSyncDisabled())
                continue;

            // Store account ID
            $accountId = (int)$event->get('account_id');
            $accountIds[$accountId] = $accountId;

            // Prepare history record if required
            if (!isset($histories[$accountId])) {
                $history = TicketModel::getModelInstance('tmsync_history');
                $history->set('broker_id', $this->brokerId);
                $history->set('account_id', $accountId);
                $history->set('sync_date', $startTime);
                $history->set('status', LocalticketsModelTmsync_history::STATUS_INCOMPLETE);
                if (!$history->save()) {
                    return $this->setError('Could not create new sync history record for account ID '.$accountId.': '.$history->getError());
                }
                $histories[$accountId] = $history;
            }

            // Set cached account to event model
            $account = $this->getAccount($accountId);
            $event->setAccount($account);

            // Download sync event items if not downloaded yet
            if (!$event->isDownloaded()) {
                if (!$this->downloadSyncEventItems($event)) {
                    continue;
                }
            }

            // Import event
            $this->importSyncEvent($event, $histories[$accountId]);
        }

        // Update all accounts with last sync date
        if ($accountIds) {
            /** @var LocalticketsModelTmsync_account $accountModel */
            $accountModel = TicketModel::getCachedInstance('tmsync_account');
            $accountModel->setAccountsSyncFinished($accountIds, $startTime);
        }

        // We can clear downloaded events now
        $this->getDownloader()->clearEvents();

        // Set history status to complete for all history records
        if ($histories) {
            $ids = TicketsCoreHelper::arrayProjection($histories, 'history_id');
            $query = "UPDATE `tmsync_history` SET `status` = ".LocalticketsModelTmsync_history::STATUS_COMPLETE." WHERE `history_id` IN (".implode(',', $ids).")";
            $this->db->setQuery($query);
            $this->db->execute();
        }

        return true;
    }

    /**
     * Downloads SyncEventSeats for given SyncEvent
     *
     * @param LocalticketsModelTmsync_event $event
     * @return bool
     */
    protected function downloadSyncEventItems($event)
    {
        // Get event account, but don't load it
        $account = $event->getAccount(false);
        if (!$account) {
            return $this->setError('No account for SyncEvent.');
        }
        if ($account->isInvalidCredentials()) {
            return $this->setError('Invalid credentials for account.');
        }

        // Download event items
        $api = $this->getApi();
        if (!$api) {
            TicketsLogger::log(TicketsLogger::TMSYNC_EVENTS_DOWNLOAD, TicketsLogger::ERROR, 'Could not load SyncEventSeats list: '.$this->getError());
            return false;
        }

        $syncItems = $api->syncEventSeatsGet($account->get('credential_id'), $event->get('sync_item_id'));
        if ($syncItems === false) {
            // Error
            $msg = $api->getApiErrorMsg() ?: $api->getError();
            TicketsLogger::log(TicketsLogger::TMSYNC_EVENTS_DOWNLOAD, TicketsLogger::ERROR, 'Could not load SyncEventSeats list: '.$msg, $api->getCommunication());
            return $this->setError('Error while loading sync event seats.');
        }

        // Add items to event
        foreach ($syncItems as $syncItem) {
            // Convert item data to our model
            $item = $this->getDownloader()->parseSyncEventItem($syncItem);
            $event->addItem($item);
        }

        return true;
    }

    /**
     * Prepares history record for account import
     *
     * @param LocalticketsModelTmsync_account $account
     * @return LocalticketsModelTmsync_history|bool
     */
    private function prepareHistoryRecord($account)
    {
        /** @var LocalticketsModelTmsync_history $history */
        $history = TicketModel::getModelInstance('tmsync_history');
        $history->set('broker_id', $this->brokerId);
        $history->set('account_id', $account->getId());
        $history->set('sync_date', gmdate('Y-m-d H:i:s'));
        $history->set('status', LocalticketsModelTmsync_history::STATUS_INCOMPLETE);
        if (!$history->save()) {
            return $this->setError('Could not create history record for account: '.$history->getError());
        }

        return $history;
    }

    /**
     * Imports single SyncEvent for specified account
     *
     * @param LocalticketsModelTmsync_account $account
     * @param string $syncItemId
     * @param int $syncQueueId
     * @return bool
     */
    public function importSingleEvent($account, $syncItemId, $syncQueueId)
    {
        // Get account credentials
        if ($account->isInvalidCredentials())
            return $this->setError('Account credentials not valid.');

        // Prepare history record for this account
        $history = $this->prepareHistoryRecord($account);
        if (!$history) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR, $this->getError());
            return false;
        }

        // Get API
        $api = $this->getApi();
        if (!$api) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR, 'Could not import account: '.$this->getError());
            return false;
        }

        // Prepare SyncEvent model for items download
        /** @var LocalticketsModelTmsync_event $event */
        $event = TicketModel::getModelInstance('tmsync_event');
        $event->addData([
            'broker_id' => $this->brokerId,
            'account_id' => $account->getId(),
            'sync_item_id' => $syncItemId,
            'sync_queue_id' => $syncQueueId,
        ]);
        $event->setAccount($account);

        // Download sync event items
        $this->notifyProcess();
        if (!$this->downloadSyncEventItems($event)) {
            // Not downloaded
            return false;
        }

        // Set ProductionID from the first item
        $item = $event->getFirstItem();
        if ($item) {
            $event->set('ProductionID', $item->get('ProductionID'));
        }

        // Import event
        $this->importSyncEvent($event, $history);

        // Set history record status as completed
        $history->set('status', LocalticketsModelTmsync_history::STATUS_COMPLETE);
        $history->updateColumns(['status']);

        return true;
    }

    /**
     * Imports all available events for given account, used by daily sync
     *
     * @param LocalticketsModelTmsync_account $account
     * @return bool
     */
    public function importAccount($account)
    {
        // Get account credentials
        if ($account->isInvalidCredentials())
            return $this->setError('Account credentials not valid.');

        // Prepare history record for this account
        $history = $this->prepareHistoryRecord($account);
        if (!$history) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR, $this->getError());
            return false;
        }

        // Load events by pages and import them
        $api = $this->getApi();
        if (!$api) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR, 'Could not import account: '.$this->getError());
            return false;
        }

        $page = 0;
        $pageSize = 100;
        while (true) {
            $this->notifyProcess();

            $events = $api->syncEventGet($credentialId, $page, $pageSize);
            if ($events === false) {
                // Error
                $msg = $api->getApiErrorMsg() ?: $api->getError();
                TicketsLogger::log(TicketsLogger::TMSYNC_EVENTS_DOWNLOAD, TicketsLogger::ERROR, 'Could not load SyncEvents list: '.$msg, $api->getCommunication());
                return $this->setError('Error while loading sync events.');
            }

            // Loop through events and import them
            foreach ($events as $syncEvent) {
                // Skip if event was already synced
                // #24337-22 dajo: Don't skip, re-import all events, even already imported
                //if ($this->isEventSynced($syncEvent->SyncItemId))
                //    continue;

                $this->notifyProcess();

                // Build correct sync event structure for tickets import
                $event = $this->getDownloader()->parseSyncEvent($syncEvent);
                $event->addData([
                    'broker_id' => $this->brokerId,
                    'account_id' => $account->getId(),
                    'ProductionID' => $syncEvent->ProductionID,
                ]);
                $event->setAccount($account);

                // #27210: Skip event if sync is disabled
                if ($event->isSyncDisabled())
                    continue;

                // Download sync event items
                if (!$this->downloadSyncEventItems($event)) {
                    // Skip event
                    continue;
                }

                // Import event
                $this->importSyncEvent($event, $history);
            }

            // If there are no more sync events, stop the process
            if (count($events) < $pageSize) {
                break;
            }

            // Load next page
            $page++;
        }

        // Set history record status as completed
        $history->set('status', LocalticketsModelTmsync_history::STATUS_COMPLETE);
        $history->updateColumns(['status']);

        return true;
    }

    /**
     * Checks whether sync event with given sync item ID was already synced before
     *
     * @param string $syncItemId
     * @return bool
     */
    protected function isEventSynced($syncItemId)
    {
        $query = "SELECT `id` FROM `tmsync_synced_event` WHERE `broker_id` = ".$this->brokerId." AND `sync_item_id` = ".$this->db->quote($syncItemId);
        $this->db->setQuery($query);
        $id = $this->db->loadResult();
        if (!$id)
            return false;

        return true;
    }

    /**
     * Stores given sync item ID in a table with synced events
     *
     * @param string $syncItemId
     * @return bool
     */
    protected function setEventSynced($syncItemId)
    {
        /** @var TableTmsync_synced_event $table */
        $table = JTable::getInstance('Tmsync_synced_event', 'Table');
        $table->broker_id = $this->brokerId;
        $table->sync_item_id = $syncItemId;
        if (!$table->store())
            return false;

        return true;
    }

    /**
     * Returns cached instance of SyncEvent items merger
     *
     * @return SyncItemsMerger
     */
    protected function getItemsMerger()
    {
        static $merger = null;

        if (!$merger) {
            $merger = new SyncItemsMerger();
        }

        return $merger;
    }

    /**
     * Imports given sync event with its items
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_history $history
     */
    protected function importSyncEvent($event, $history)
    {
        // Check if event was already imported
        // #24337-22: Don't check this, re-import already imported events
        //if ($this->isEventSynced($event->get('sync_item_id'))) {
        //    $this->importAlreadySyncedEvent($event);
        //    return;
        //}

        // Just to be sure - check if account specified
        $account = $event->getAccount(false);
        if (!$account) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Skipped event with no Sync account specified.');
            return;
        }

        // Skip if there are no items
        $items = $event->getItems();
        if (!$items || !is_array($items)) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Skipped event with no items (SyncItemID '.$event->get('sync_item_id').')');
            return;
        }

        $this->notifyProcess();

        // Try to map event if required
        if (!$event->get('event_id')) {
            if (!$event->get('ProductionID')) {
                // No EI Production ID, can't map event
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Skipped event with no production ID (SyncItemID '.$event->get('sync_item_id').')');
                return;
            }

            // Map event
            $mapper = $this->getMapper();
            if (!$mapper) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not map event: '.$this->getError());
                return;
            }

            $result = $mapper->findOrCreateEiEvent($event->get('ProductionID'));
            if (!$result) {
                // Log error
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Error while mapping event: '.$mapper->getError());
                return;
            }
            if (!$result['event_id']) {
                // Event not mapped
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Skipped unmapped event (SyncItemID '.$event->get('sync_item_id').')');
                return;
            }

            // Event mapped
            $event->set('event_id', $result['event_id']);
        }

        $this->notifyProcess();

        // Prepare new history event record
        /** @var LocalticketsModelTmsync_history_event $historyEvent */
        $historyEvent = TicketModel::getModelInstance('tmsync_history_event');
        $historyEvent->addData($event->getProperties());
        $historyEvent->set('sync_date', gmdate('Y-m-d H:i:s'));
        $historyEvent->set('history_id', $history->getId());

        // #23487: Merge items for odd/even seats
        $items = $this->getItemsMerger()->mergeItems($items);

        // Precalculate total cost for event items
        $this->precalculateItemsTotalCost($items);

        $this->notifyProcess();

        // Prepare PO for this event if set to
        $po = null;
        if (!$this->isMatchOnly($account)) {
            $po = $this->preparePo($event);
        }

        // #28403: Special handling for IsFrontGate events (GA tickets)
        /** @var TableEvent $eventTable */
        $eventTable = JTable::getInstance('Event', 'Table');
        if (!$eventTable->load($event->get('event_id'))) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not load event ID '.$event->get('event_id').' (SyncItemID '.$event->get('sync_item_id').')');
            return;
        }

        // #28607: Find related parking event
        $event->set('parking_event_id', $this->getRelatedParkingEvent($eventTable));

        if ($eventTable->event_front_gate) {
            $allTgs = $this->importGaSyncEvent($event, $items, $historyEvent, $po);
        }
        else {
            // Loop through items and map them to existing tickets or create new tickets
            $allTgs = [];
            foreach ($items as $item) {
                $this->notifyProcess();

                $tgs = $this->importSyncEventItem($event, $item, $historyEvent, $po);
                if ($tgs) {
                    $allTgs = array_merge($allTgs, $tgs);
                }
            }
        }

        // Add total fees and delivery to PO from first item
        if ($po) {
            $item = reset($items);
            if ($item) {
                $po->set('fee_api', round($po->get('fee_api', 0) + $item->get('total_fees'), 2));
                $po->getPrimaryShipping()->set('shipping_cost', round($item->get('total_delivery'), 2));
            }
        }

        // Save purchase order if prepared and contains any items
        if ($po) {
            if ($po->getItems()) {
                $po->collectTotals();
                if (!$po->save()) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR, 'Could not create purchase order (SyncItemID '.$event->get('sync_item_id').'): '.$po->getError());
                }
                else {
                    $po->logOrderCreation(true, $account->get('broker_id'));

                    // #25169: Confirm new PO
                    $po->confirm();
                }
            }

            $po->releaseReferences();
            $po = null;
        }

        // Save history record
        $historyEvent->save();

        $this->notifyProcess();

        // Upload ticket groups to EI/TM
        if ($allTgs) {
            $uploader = $this->getTmUploader();
            if ($uploader) {
                // TODO: Handle errors somehow?
                $uploader->uploadTickets($allTgs);
            }
        }

        $this->notifyProcess();

        // #25801: Set all TGs as Active after the TM uploader was run
        $this->setTicketsActive($allTgs);

        // Post the SyncEventQueue to EI/TM API - use account from event model
        if ($account) {
            // 27883: Don't start new sync queue for single event import if existing SyncQueueID given
            $syncQueueId = $event->get('sync_queue_id');
            if ($syncQueueId) {
                // #28145: SyncQueue is already running on TM, display "In Progress..." on accounts list
                $account->setSyncQueueRunning();
            }
            else {
                // Prepare item info to store in the SyncQueue
                $itemInfo = [
                    'EventName' => $event->get('event_name'),
                    'EventDate' => $event->get('event_date'),
                    'EventTba' => $event->get('event_tba') ? 1 : 0,
                    'VenueName' => $event->get('venue_name'),
                ];
                $api = $this->getApi();
                if (!$api) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not start new SyncQueue (SyncItemID '.$event->get('sync_item_id').'): '.$this->getError());
                }
                else {
                    $queueId = $api->syncQueuePost($account->get('credential_id'), $event->get('sync_item_id'), $itemInfo);
                    if ($queueId) {
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'New SyncQueue (SyncItemID '.$event->get('sync_item_id').') started (ID '.$queueId.')', $api->getCommunication());

                        // SyncQueue executed, store new queue ID
                        $historyEvent->set('sync_queue_id', $queueId);
                        $historyEvent->updateColumns(['sync_queue_id']);

                        // Set that SyncQueue is running for TM Sync account
                        $account->setSyncQueueRunning();
                    }
                    else {
                        $msg = $api->getApiErrorMsg();
                        if (!$msg)
                            $msg = $api->getError();
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not start new SyncQueue (SyncItemID '.$event->get('sync_item_id').'): '.$msg, $api->getCommunication());
                    }
                }
            }
        }

        // Flag event as already synced
        $this->setEventSynced($event->get('sync_item_id'));
    }

    /**
     * Finds related parking event to given event
     *
     * @param TableEvent $event
     * @return int
     */
    protected function getRelatedParkingEvent($event)
    {
        if (!$event->event_ext_tm_id)
            return null;

        // Negate the parking flag
        $tmExtId = $event->event_ext_tm_id;
        $parking = !$event->event_parking;

        $query = "SELECT `event_id` FROM `events`
            WHERE `event_ext_tm_id` = ".(int)$tmExtId."
              AND `event_parking` = ".($parking ? 1 : 0);
        $this->db->setQuery($query);
        $eventId = $this->db->loadResult();
        if (!$eventId)
            return null;

        return (int)$eventId;
    }

    /**
     * Sets given tickets from Pending to Active
     *
     * @param int[] $ticketIds
     */
    protected function setTicketsActive($ticketIds)
    {
        if (!$ticketIds)
            return;

        $query = "UPDATE `ticket`
            SET `ticket_status` = ".TicketStatus::ACTIVE."
            WHERE `ticket_id` IN (".implode(',', $ticketIds).")
              AND `ticket_status` = ".TicketStatus::PENDING;
        $this->db->setQuery($query);
        $this->db->execute();
    }

    /**
     * #23057: Skips import of already synced event, but issues new SyncQueue for it
     * and schedules SyncQueue checks of previously imported tickets
     *
     * @param LocalticketsModelTmsync_event $event
     */
    protected function importAlreadySyncedEvent($event)
    {
        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Skipping import of already synced event (SyncItemID '.$event->get('sync_item_id').')');

        $account = $event->getAccount(false);
        if (!$account) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'No account for already synced event (SyncItemID '.$event->get('sync_item_id').')');
            return;
        }

        // Post new sync queue
        $api = $this->getApi();
        if (!$api) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not start new SyncQueue (SyncItemID '.$event->get('sync_item_id').'): '.$this->getError());
            return;
        }

        $itemInfo = [
            'EventName' => $event->get('event_name'),
            'EventDate' => $event->get('event_date'),
            'EventTba' => $event->get('event_tba') ? 1 : 0,
            'VenueName' => $event->get('venue_name'),
        ];
        $queueId = $api->syncQueuePost($account->get('credential_id'), $event->get('sync_item_id'), $itemInfo);
        if ($queueId) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'New SyncQueue (SyncItemID '.$event->get('sync_item_id').') started (ID '.$queueId.')', $api->getCommunication());

            // Set that SyncQueue is running for TM Sync account
            $account->setSyncQueueRunning();
        }
        else {
            $msg = $api->getApiErrorMsg();
            if (!$msg)
                $msg = $api->getError();
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not start new SyncQueue (SyncItemID '.$event->get('sync_item_id').'): '.$msg, $api->getCommunication());
        }
    }

    /**
     * Prepares new PO model for given SyncEvent
     *
     * @param LocalticketsModelTmsync_event $event
     * @return LocalticketsModelOrder2
     */
    protected function preparePo($event)
    {
        $account = $event->getAccount(false);

        /** @var LocalticketsModelOrder2 $po */
        $po = TicketModel::getModelInstance('order2');
        $po->addData([
            'broker_id' => $account->get('po_vendor_id'),
            'customer_id' => $account->get('broker_id'),
            'transaction_date' => gmdate('Y-m-d H:i:s'),
            'source' => 'api',
            'status' => LocalticketsModelOrder2::STATUS_PENDING
        ]);

        // Add private note if set
        if ($account->get('po_internal_note')) {
            /** @var LocalticketsModelOrder2_note $note */
            $note = TicketModel::getInstance('order2_note');
            $note->addData(array(
                'public'   => 0,
                'text'     => $account->get('po_internal_note')
            ));
            $po->addNote($note);
        }

        // Load event data
        $eventId = $event->get('event_id');
        $eventData = TicketModel::getModelInstance('event');
        $eventData->load($eventId);

        // Prepare event info
        /** @var LocalticketsModelOrder2_event_info $eventInfo */
        $eventInfo = TicketModel::getModelInstance('order2_event_info');
        $eventInfo->set('event_id', $eventId);
        $eventInfo->set('name', $eventData->get('event_name'));
        $eventInfo->set('date', $eventData->get('event_date'));
        $eventInfo->set('venue', $eventData->get('event_venue'));
        $eventInfo->set('city', TicketsHelper::getCityName($eventData->get('event_city_id')));
        $eventInfo->set('state', $eventData->get('event_state'));
        $eventInfo->set('tba', $eventData->get('event_tba'));

        $po->set('_event_info', $eventInfo);

        // Create primary shipping, so we can set delivery price
        /** @var LocalticketsModelOrder2_shipping $shipping */
        $shipping = TicketModel::getModelInstance('order2_shipping');
        $shipping->set('type', Shippings::EXCHANGE);
        $po->setPrimaryShipping($shipping);

        // #29157: Add tags from account
        $tags = $account->getOrderTags();
        foreach ($tags as $tag) {
            $po->addTag($tag);
        }

        // Set as shipped
        $shipping->setShipped();

        return $po;
    }

    /**
     * Adds newly created ticket to PO
     *
     * @param LocalticketsModelOrder2 $po
     * @param TableTicket $ticket
     */
    protected function addTicketToPo($po, $ticket)
    {
        // Get prepared event info from PO
        $eventInfo = $po->get('_event_info');

        // Prepare new order item
        /** @var LocalticketsModelOrder2_item $item */
        $item = TicketModel::getModelInstance('order2_item');
        $item->set('ticket_id', $ticket->ticket_id);
        $item->set('ticket_broker_id', $ticket->ticket_broker_id);
        $item->set('price_per_ticket', $ticket->get('_ticket_cost_clean'));
        $item->set('section', $ticket->ticket_section);
        $item->set('row', $ticket->ticket_row);
        $item->set('seat_from', $ticket->ticket_seat);
        $item->set('seat_through', $ticket->ticket_seat_through);
        $item->set('seats', $ticket->ticket_seats_overall);
        $item->set('quantity', $ticket->ticket_available_now);
        $item->set('eticket', $ticket->ticket_eticket);
        $item->setEventInfo($eventInfo);

        // Add item to PO
        $po->addItem($item);
    }

    /**
     * Searches for possible matching ticket groups
     *
     * @param int[] $eventIds
     * @param string $section
     * @param string $row
     * @param string $syncItemId
     * @param bool $onlyGa
     * @return LocalticketsModelTicket[]
     */
    protected function findAvailableTicketGroups(array $eventIds, $section, $row, $syncItemId = null, $onlyGa = false)
    {
        if (!$eventIds)
            return [];
        $eventIds = TicketsCoreHelper::arrayToInts($eventIds);

        /** @var LocalticketsModelTicketcollection $collection */
        $collection = TicketModel::getCollectionInstance('ticket');
        $collection->addFilter(["ticket_broker_id", "= ".(int)$this->brokerId]);
        $collection->addFilter(["ticket_event_id", "IN (".implode(',', $eventIds).")"]);
        $collection->addFilter(["ticket_section", "LIKE ".$collection->Quote($section)]);
        $collection->addFilter(["ticket_row", "LIKE ".$collection->Quote($row)]);
        $collection->addFilter(["ticket_status", "IN (".TicketStatus::getConfirmed().")"]);
        if ($syncItemId)
            $collection->addFilter(["ticket_sync_item_id", "= ".$collection->Quote($syncItemId)]);
        if ($onlyGa)
            $collection->addFilter(["ticket_ga", "= 1"]);
        $collection->setLoadCounts(false);
        $collection->load();
        $groups = $collection->getItems();
        if (!$groups)
            return [];

        // Filter only ticket groups with specific seats and parse the seats
        $tgs = [];
        foreach ($groups as $group) {
            // Consider only groups with specific seats
            if ($group->get('ticket_seats_tbd'))
                continue;
            if (!$group->get('ticket_seats_original') && !$group->get('ticket_seats_overall'))
                continue;

            // Parse TG seats
            $origSeats = TicketsSeatsHelper::getSeatsFromHumanReadable($group->get('ticket_seats_original'));
            $seats = TicketsSeatsHelper::getSeatsFromHumanReadable($group->get('ticket_seats_overall'));
            if (!$seats && !$origSeats)
                continue;

            // Just to be sure that original seats are set
            if (!$origSeats)
                $origSeats = $seats;

            $group->set('_seats_array', $seats);
            $group->set('_orig_seats_array', $origSeats);
            $tgs[] = $group;
        }

        return $tgs;
    }

    /**
     * Checks if given seats array is considered odd/even, including only 2 seats
     *
     * @param int[] $seats
     * @return bool
     */
    protected function isOddEvenSeats($seats)
    {
        $cnt = count($seats);
        if ($cnt < 2)
            return false;

        // Get the first seat and generate odd/even sequence
        $seat1 = reset($seats);
        $seat2 = $seat1 + (($cnt - 1) * 2);
        $oddEven = range($seat1, $seat2, 2);

        // Compare arrays
        if (array_values($seats) == array_values($oddEven))
            return true;

        return false;
    }

    /**
     * #28403: Handles Sync for GA tickets (FrontGate events). Because there is no unique ID for SyncEventSeat items
     * in BAPI, we need to try to match existing tickets by SyncItemID. There may be multiple ticket groups with the
     * same seats, so we need to count all the seats and match them with existing tickets. Matched tickets will be
     * skipped (won't be updated), only unmatched seats will be created.
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item[] $items
     * @param LocalticketsModelTmsync_history_event $historyEvent
     * @param LocalticketsModelOrder2 $po
     * @return int[]|bool
     */
    protected function importGaSyncEvent($event, $items, $historyEvent, $po = null)
    {
        // First group items by section and row
        $itemsGroups = $this->groupGaItemsBySectionRow($event, $items);

        // Import individual groups
        $allTgs = [];
        foreach ($itemsGroups as $group) {
            $this->notifyProcess();

            $tgs = $this->importGaItemsGroup($event, $group, $historyEvent, $po);
            if ($tgs) {
                $allTgs = array_merge($allTgs, $tgs);
            }
        }

        return $allTgs;
    }

    /**
     * Groups GA sync items by section and row
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item[] $items
     * @return LocalticketsModelTmsync_item[][]
     */
    protected function groupGaItemsBySectionRow($event, $items)
    {
        $itemsGroups = [];
        foreach ($items as $item) {
            // Check item info
            $seats = TicketsSeatsHelper::getSeatsFromHumanReadable($item->get('seats'));
            if (!$item->get('section') || !$item->get('row') || !$seats) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not import sync event item (SyncItemID '.$event->get('sync_item_id').'): Section, row or seats missing');
                continue;
            }

            // Group
            $key = $item->get('section').'::'.$item->get('row');
            if (!isset($itemsGroups[$key])) {
                $itemsGroups[$key] = [];
            }
            $itemsGroups[$key][] = $item;
        }

        return $itemsGroups;
    }

    /**
     * Imports group of GA sync event items with the same section and row
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item[] $items
     * @param LocalticketsModelTmsync_history_event $historyEvent
     * @param LocalticketsModelOrder2 $po
     * @return int[]
     */
    protected function importGaItemsGroup($event, $items, $historyEvent, $po = null)
    {
        // Get available GA ticket groups created from this SyncEvent
        $item = reset($items);
        $section = $item->get('section');
        $row = $item->get('row');

        // Search in both normal event and related parking event
        $eventIds = [$event->get('event_id')];
        if ($event->get('parking_event_id'))
            $eventIds[] = $event->get('parking_event_id');

        $tgs = $this->findAvailableTicketGroups($eventIds, $section, $row, $event->get('sync_item_id'), true);

        // Loop through items and first try to match them to existing tickets exactly
        foreach ($items as $itemKey => $item) {
            $allSeats = TicketsSeatsHelper::getSeatsFromHumanReadable($item->get('seats'));

            // Try to match some TG exactly
            foreach ($tgs as $key => $tg) {
                $tgSeats = $tg->get('_orig_seats_array');
                if ($allSeats == $tgSeats) {
                    // Exact match, store in history
                    /** @var LocalticketsModelTmsync_history_item $historyItem */
                    $historyItem = TicketModel::getModelInstance('tmsync_history_item');
                    $historyItem->addData($item->getProperties());
                    $historyItem->set('ticket_ids', [$tg->getId()]);
                    $historyEvent->addItem($historyItem);

                    // Log
                    $matchedSeatsLog = TicketsSeatsHelper::getSeatsToHumanReadable($allSeats);
                    $details = $this->getLogTicketInfo($event, $item, $allSeats);
                    $msg = 'Seat(s) '.$matchedSeatsLog.' (GA) matched exactly to TG ID '.$tg->getId().', skipping';
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, $msg, $details);

                    // Remove this item and TG from lists
                    unset($tgs[$key]);
                    unset($items[$itemKey]);
                    break;
                }
            }

        }

        // List of newly created tickets to upload to TM
        $ticketIds = [];

        // Now loop through remaining items and try to match individual seats
        foreach ($items as $item) {
            // Prepare history record
            /** @var LocalticketsModelTmsync_history_item $historyItem */
            $historyItem = TicketModel::getModelInstance('tmsync_history_item');
            $historyItem->addData($item->getProperties());
            $historyEvent->addItem($historyItem);

            $allSeats = TicketsSeatsHelper::getSeatsFromHumanReadable($item->get('seats'));

            $matchedTgsIds = [];
            $unmatchedSeats = $allSeats;
            foreach ($tgs as $tg) {
                $tgSeats = $tg->get('_orig_seats_array');
                $matchedSeats = array_values(array_intersect($tgSeats, $unmatchedSeats));
                if ($matchedSeats) {
                    // There are some matched seats, remember this TG and remove seats from unmatched
                    $unmatchedSeats = array_values(array_diff($unmatchedSeats, $matchedSeats));
                    $matchedTgsIds[] = $tg->getId();

                    // Remove also from available seats from this TG
                    $remainingSeats = array_values(array_diff($tgSeats, $matchedSeats));
                    $tg->set('_orig_seats_array', $remainingSeats);

                    // Log
                    $matchedSeatsLog = TicketsSeatsHelper::getSeatsToHumanReadable($matchedSeats);
                    $details = $this->getLogTicketInfo($event, $item, $allSeats);
                    $msg = 'Seat(s) '.$matchedSeatsLog.' (GA) matched to TG ID '.$tg->getId();
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, $msg, $details);

                    // End loop if no more unmatched seats
                    if (!$unmatchedSeats)
                        break;
                }
            }

            $this->notifyProcess();

            // Create new ticket group for unmatched seats
            if ($unmatchedSeats) {
                // #31822: Only if PO available
                if (!$po) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Unmatched seats, but Match Only is enabled', $this->getLogTicketInfo($event, $item, $unmatchedSeats));
                }
                else {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Unmatched seats, creating new ticket group', $this->getLogTicketInfo($event, $item, $unmatchedSeats));
                    $ticket = $this->createTicket($event, $item, $unmatchedSeats, true);
                    if (!$ticket) {
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, $this->getError());
                    }
                    else {
                        // Add ticket to PO
                        $this->addTicketToPo($po, $ticket);
                        $fees = $item->get('fee') * $ticket->ticket_available_now;
                        $tax = $item->get('tax') * $ticket->ticket_available_now;
                        $po->set('fee_api', $po->get('fee_api', 0) + $fees);
                        $po->set('tax', $po->get('tax', 0) + $tax);

                        $matchedTgsIds[] = $ticket->ticket_id;
                        $ticketIds[] = $ticket->ticket_id;
                    }
                }
            }

            // Store matched TGs
            $historyItem->set('ticket_ids', $matchedTgsIds);
        }

        return $ticketIds;
    }

    /**
     * Imports single event item, returns matched or created TG
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item $item
     * @param LocalticketsModelTmsync_history_event $historyEvent
     * @param LocalticketsModelOrder2 $po PO for newly created tickets
     * @return int[]|bool
     */
    protected function importSyncEventItem($event, $item, $historyEvent, $po = null)
    {
        // Check item info
        $allSeats = TicketsSeatsHelper::getSeatsFromHumanReadable($item->get('seats'));
        if (!$item->get('section') || !$item->get('row') || !$allSeats) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not import sync event item (SyncItemID '.$event->get('sync_item_id').'): Section, row or seats missing');
            return $this->setError('Section, row or seats missing');
        }

        // Prepare history event item
        /** @var LocalticketsModelTmsync_history_item $historyItem */
        $historyItem = TicketModel::getModelInstance('tmsync_history_item');
        $historyItem->addData($item->getProperties());
        $historyEvent->addItem($historyItem);

        // First search for all ticket groups matching section and row
        // Search in both normal event and related parking event
        $eventIds = [$event->get('event_id')];
        if ($event->get('parking_event_id'))
            $eventIds[] = $event->get('parking_event_id');
        $tgs = $this->findAvailableTicketGroups($eventIds, $item->get('section'), $item->get('row'));

        // Find matching ticket group for each seat
        /** @var LocalticketsModelTicket[] $matchedTgs */
        $matchedTgs = [];
        $unmatchedSeats = $allSeats;
        foreach ($tgs as $tg) {
            $tgSeats = $tg->get('_orig_seats_array');
            $matchedSeats = array_values(array_intersect($tgSeats, $unmatchedSeats));
            if ($matchedSeats) {
                // There are some matched seats, remember this TG and remove seats from unmatched
                $tg->set('_matched_seats', $matchedSeats);
                $unmatchedSeats = array_values(array_diff($unmatchedSeats, $matchedSeats));
                $matchedTgs[] = $tg;

                // End loop if no more unmatched seats
                if (!$unmatchedSeats)
                    break;
            }
        }

        /** @var LocalticketsModelTicket $ticketModel */
        $ticketModel = TicketModel::getCachedInstance('ticket');

        // Get account for Auto PO settings
        $account = $event->getAccount(false);

        // List of modified tickets
        $ticketIds = [];

        // #26575-7: If there's only one matched TG and there are some unmatched seats, check if the TG can be expanded
        // #31822: Only if "Match Only" not enabled
        $expanded = false;
        if ($po && $unmatchedSeats && count($matchedTgs) == 1) {
            $tg = reset($matchedTgs);
            if ($this->canExpandMatchedTicketGroup($unmatchedSeats, $allSeats, $tg)) {
                // Expand matched TG
                $newTgSeats = TicketsSeatsHelper::computeNewSeats('add', $tg->get('ticket_seats_overall'), $unmatchedSeats, $tg->get('ticket_seats_extra'));
                $newTgOrigSeats = TicketsSeatsHelper::computeNewSeats('add', $tg->get('ticket_seats_original'), $unmatchedSeats);

                // Log
                $matchedSeatsLog = TicketsSeatsHelper::getSeatsToHumanReadable($tg->get('_matched_seats'));
                $details = $this->getLogTicketInfo($event, $item, $allSeats);
                $msg = 'Seats '.$matchedSeatsLog.' matched partially, expanding TG ID '.$tg->getId();
                $details .= "\n\nNew TG seats: ".TicketsSeatsHelper::getSeatsToHumanReadable($newTgSeats['seats']);
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, $msg, $details, null, $tg->getId());

                /** @var TableTicket $former */
                $former = JTable::getInstance('Ticket', 'Table');
                $former->load($tg->getId());
                /** @var TableTicket $table */
                $table = JTable::getInstance('Ticket', 'Table');
                $table->bind($former);
                $table->ticket_seats_original = TicketsSeatsHelper::getSeatsToHumanReadable($newTgOrigSeats['seats']);
                $table->ticket_seats_overall = TicketsSeatsHelper::getSeatsToHumanReadable($newTgSeats['seats']);
                $table->ticket_seat = $newTgSeats['seat_from'];
                $table->ticket_seat_through = $newTgSeats['seat_through'];
                $extra = $table->getTicketSeatsExtra();
                if (!is_object($extra)) {
                    $extra = new stdClass();
                }
                $extra->aisle = $newTgSeats['aisle'];
                $table->ticket_seats_extra = $extra;
                $table->ticket_total_available = count($newTgOrigSeats['seats']);
                $table->ticket_available_now = count($newTgSeats['seats']);

                // #25801: Set TG as Pending to ignore it in normal uploaders until it is updated by our TM Sync uploader
                $table->ticket_status = TicketStatus::PENDING;

                // Update auto PO settings for existing ticket if set to
                // #29191: Always update auto PO settings
                // #29348: NEVER update auto PO settings
                /*if ($account) {
                    $table->ticket_cost_price = round((float)$item->get('total_cost'), 2);
                    $this->setTicketAutoPoOptions($table, $account);
                }*/

                if (!$table->store()) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not expand existing ticket group (ID: '.$tg->getId().'): '.$table->getError());
                    return $this->setError('Could not expand ticket group (ID: '.$tg->getId().')');
                }

                // Clear unmatched seats
                $expanded = true;
                $unmatchedSeats = [];
                $ticketIds[] = $tg->getId();

                // Propagate to PO
                // #26529-9: Only if there's not multiple linked POs to the ticket
                if (!$ticketModel->hasMultiplePurchaseOrderItems($table)) {
                    if (!$ticketModel->propagateToPO($former, $table)) {
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not propagate expanded ticket group to PO (ID: '.$tg->getId().'): '.$ticketModel->getError());
                    }
                }

                // Update ticket's PO or create new
                if ($account && !$account->isAutoPoOnlyNew()) {
                    $this->updateTicketPo($tg->getId(), $event, $item);
                }

                $this->notifyProcess();
            }
        }

        // Handle matched TGs if not handled during TG expand
        if (!$expanded) {
            foreach ($matchedTgs as $tg) {
                $this->notifyProcess();

                // Log
                $matchedSeatsLog = TicketsSeatsHelper::getSeatsToHumanReadable($tg->get('_matched_seats'));
                $details = $this->getLogTicketInfo($event, $item, $allSeats);
                $msg = 'Seat(s) '.$matchedSeatsLog.' matched to TG ID '.$tg->getId();
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, $msg, $details);

                // Update auto PO settings for existing ticket if set to
                // #29191: Always update auto PO settings
                // #29348: NEVER update auto PO settings
                /*if ($account) {*/
                    /** @var TableTicket $ticketTable */
                    /*$ticketTable = \JTable::getInstance('Ticket', 'Table');
                    $ticketTable->ticket_id = $tg->getId();
                    $ticketTable->ticket_cost_price = round((float)$item->get('total_cost'), 2);
                    $this->setTicketAutoPoOptions($ticketTable, $account);
                    if (!$ticketTable->store()) {
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not update existing ticket group (ID: '.$tg->getId().'): '.$ticketTable->getError());
                        return $this->setError('Could not update ticket group (ID: '.$tg->getId().')');
                    }

                    // Update ticket's PO or create new
                    $this->updateTicketPo($tg->getId(), $event, $item);
                }*/

                $ticketIds[] = $tg->getId();
            }
        }

        $this->notifyProcess();

        // Collect fees and tax
        $fees = 0;
        $tax = 0;

        // Create new ticket group for unmatched seats
        if ($unmatchedSeats) {
            // #31822: Only if PO available
            if (!$po) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Unmatched seats, but Match Only is enabled', $this->getLogTicketInfo($event, $item, $unmatchedSeats));
            }
            else {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Unmatched seats, creating new ticket group', $this->getLogTicketInfo($event, $item, $unmatchedSeats));
                $ticket = $this->createTicket($event, $item, $unmatchedSeats);
                if (!$ticket) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, $this->getError());
                }
                else {
                    // Add ticket to PO
                    if ($po) {
                        $this->addTicketToPo($po, $ticket);
                        $fees += $item->get('fee') * $ticket->ticket_available_now;
                        $tax += $item->get('tax') * $ticket->ticket_available_now;
                    }

                    $ticketIds[] = $ticket->ticket_id;
                }
            }
        }

        // Store matched TGs
        $historyItem->set('ticket_ids', $ticketIds);

        // Add fees and tax for current item to order
        if ($po) {
            $po->set('fee_api', $po->get('fee_api', 0) + $fees);
            $po->set('tax', $po->get('tax', 0) + $tax);
        }

        return $ticketIds;
    }

    /**
     * Checks whether single matched TG can be expanded to contain all unmatched seats
     *
     * @param int[] $unmatchedSeats
     * @param int[] $allSeats
     * @param LocalticketsModelTicket $tg
     * @return bool
     */
    private function canExpandMatchedTicketGroup($unmatchedSeats, $allSeats, $tg)
    {
        // Check if there are any unsold seats left in the TG
        if (!$tg->get('_seats_array'))
            return false;

        // Check seats type so we don't accidentally expand odd/even TG with consecutive seats
        $tgSeats = $tg->get('_seats_array');
        $itemSeatsType = TicketsSeatsHelper::getSeatsType($allSeats);
        $tgSeatsType = TicketsSeatsHelper::getSeatsType($tgSeats, $tg->get('ticket_even_odd'));
        if ($itemSeatsType != $tgSeatsType)
            return false;

        // Merge seats and check if they form a valid TG
        $newSeats = array_merge($unmatchedSeats, $tgSeats);
        if (!TicketsSeatsHelper::isContinuous($newSeats))
            return false;

        return true;
    }

    /**
     * Returns string representation of event for logging details
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item $item
     * @param array $seats
     * @return string
     */
    private function getLogTicketInfo($event, $item, $seats)
    {
        $info = $event->get('event_name').', '.$event->get('event_date').($event->get('event_tba') ? ' (TBA)' : '').', '.$event->get('venue_name');
        $info .= "\n";
        $info .= 'Section: '.$item->get('section').', Row: '.$item->get('row').', Seats: '.TicketsSeatsHelper::getSeatsToHumanReadable($seats);

        return $info;
    }

    /**
     * Updates or creates new PO for existing ticket using settings from given event
     *
     * @param int $ticketId
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item $item
     */
    protected function updateTicketPo($ticketId, $event, $item)
    {
        // Get account settings
        $account = $event->getAccount(false);

        /** @var LocalticketsModelTicket $ticketModel */
        $ticketModel = TicketModel::getCachedInstance('ticket');

        /** @var TableTicket $ticket */
        $ticket = JTable::getInstance('Ticket', 'Table');
        if (!$ticket->load($ticketId))
            return;

        // #26529-9: Only if there's not multiple linked POs to the ticket
        $poItems = $ticketModel->getAllPurchaseOrderItems($ticket);
        if ($poItems && count($poItems) > 1)
            return;

        $poItem = ($poItems ? reset($poItems) : null);
        if ($poItem) {
            // Get PO
            $po = $poItem->getOrder();
            if (!$po) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR,
                    'Could not load purchase order for existing ticket group (SyncItemID '.$event->get('sync_item_id').'): '.$po->getError(),
                    null, null, $ticketId);
                return;
            }

            // Update PO only if V2B order
            if ($po->getOrderType() != LocalticketsModelOrder2::ORDER_TYPE_V2B) {
                return;
            }

            // Update vendor
            if ($po->get('broker_id') != $account->get('po_vendor_id')) {
                $po->set('broker_id', $account->get('po_vendor_id'));
                if (!$po->save(false, true, false)) {
                    TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR,
                        'Could not update purchase order for existing ticket group (SyncItemID '.$event->get('sync_item_id').'): '.$po->getError(),
                        null, null, $ticketId, null, $po->getId(), null, $poItem->getId());
                }
            }

            // Update private note - first search if the same note is not already there
            if ($account->get('po_internal_note')) {
                $found = false;
                $notes = $po->getNotes(0);
                foreach ($notes as $note) {
                    if ($note->get('text') == $account->get('po_internal_note')) {
                        $found = true;
                        break;
                    }
                }

                if (!$found) {
                    // Create note
                    /** @var LocalticketsModelOrder2_note $note */
                    $note = TicketModel::getInstance('order2_note');
                    $note->addData(array(
                        'order_id' => $po->getId(),
                        'public'   => 0,
                        'text'     => $account->get('po_internal_note')
                    ));
                    if (!$note->save()) {
                        TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR,
                            'Could not save note for existing purchase order (SyncItemID '.$event->get('sync_item_id').'): '.$po->getError(),
                            null, null, $ticketId, null, $po->getId(), null, $poItem->getId());
                    }
                }
            }

            $po->releaseReferences();
        }
        else {
            // Create new PO
            $po = $this->preparePo($event);

            // Store clean cost from Sync item to avoid error
            $ticket->set('_ticket_cost_clean', round((float)$item->get('cost'), 2));
            $this->addTicketToPo($po, $ticket);

            $po->collectTotals();

            // Save PO
            if (!$po->save()) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::ERROR,
                    'Could not create purchase order for existing ticket group (SyncItemID '.$event->get('sync_item_id').'): '.$po->getError(),
                    null, null, $ticketId);
            }
            else {
                $po->logOrderCreation(true, $account->get('broker_id'));
            }

            $po->releaseReferences();
        }
    }

    /**
     * Creates new ticket group from given sync event item
     *
     * @param LocalticketsModelTmsync_event $event
     * @param LocalticketsModelTmsync_item $item
     * @param array $seats
     * @param bool $ga
     * @return TableTicket|false
     */
    protected function createTicket($event, $item, $seats, $ga = false)
    {
        /** @var TableTicket $ticketTable */
        $ticketTable = JTable::getInstance('Ticket', 'Table');

        $eventId = $event->get('event_id');
        $ticketTable->ticket_event_id = $eventId;
        $ticketTable->ticket_broker_id = $this->brokerId;

        // #26704-5: Set ticket owner to currently logged in user if import run manually
        $ticketTable->ticket_user_id = $this->userId;

        // Set some default values
        /** @var LocalticketsModelTicket $ticketModel */
        $ticketModel = TicketModel::getCachedInstance('ticket');
        $ticketModel->setDefaultsForBroker($ticketTable, $this->brokerId);

        $seatsString = TicketsSeatsHelper::getSeatsToHumanReadable($seats);
        $seatsArray = TicketsSeatsHelper::getSeatsFromHumanReadable($seatsString);

        // Set actual number of tickets
        $ticketTable->ticket_total_available = count($seatsArray);
        $ticketTable->ticket_available_now = count($seatsArray);

        // Get seats from/to (deprecated)
        $from_to = TicketsSeatsHelper::getFromAndTo($seatsArray, true);

        $ticketTable->ticket_section = $item->get('section');
        $ticketTable->ticket_row = $item->get('row');
        $ticketTable->ticket_seats_overall = $seatsString;
        $ticketTable->ticket_seats_original = $seatsString;
        $ticketTable->ticket_seat = $from_to['seat_from'];
        $ticketTable->ticket_seat_through = $from_to['seat_through'];
        $ticketTable->ticket_seats_show = 1;
        $ticketTable->ticket_eticket = 0;
        $ticketTable->ticket_barcode = 0;

        $ticketTable->last_touched = gmdate('Y-m-d H:i:s');
        $ticketTable->ticket_method = 'manual';
        $ticketTable->ticket_note = '';
        $ticketTable->ticket_disclosures = [];

        // GA flag - either set as GA or keep as automatic
        $ticketTable->ticket_ga = ($ga ? 1 : -1);

        // Set cost, remember cost without any fees for PO creation
        $ticketTable->set('_ticket_cost_clean', round((float)$item->get('cost'), 2));
        $ticketTable->ticket_cost_price = round((float)$item->get('total_cost'), 2);

        // Set zero sell prices
        $ticketTable->ticket_price = 0;
        $ticketTable->ticket_price_selling = 0;

        // Make TG public, so it gets uploaded to EI/TM
        $ticketTable->ticket_private = 0;

        // #25801: Create new tickets as Pending to ignore them by normal uploaders until they're uploaded by our TM Sync uploader
        $ticketTable->ticket_status = TicketStatus::PENDING;

        // #28403: Source SyncItemID
        $ticketTable->ticket_sync_item_id = $event->get('sync_item_id');

        // Apply Auto PO settings if set to
        $account = $event->getAccount(false);
        if ($account) {
            $this->setTicketAutoPoOptions($ticketTable, $account);

            // #27450: Store Sync account info in ticket's private note
            $accountType = $account->getAccountType();
            $ticketTable->ticket_note_private = 'Created from '.($accountType ? $accountType->get('name', 'Unknown') : 'Unknown').' account - username '.$account->get('username');
        }

        // Save ticket
        if (!$ticketTable->check() || !$ticketTable->store(false, false, 'TM Sync')){
            return $this->setError('Cannot create new ticket group: '.$ticketTable->getError());
        }

        // #17884: Set default broadcast value
        $broker = TicketsHelper::getBrokerDetails($this->brokerId);
        if ($broker && $broker->broker_hide_tickets) {
            // #25876: Set broadcast for TM
            $tmMarketplaceId = Marketplaces::getTmMarketplaceId();
            $data = [$tmMarketplaceId => ['exchange_broadcast_value' => 0]];
            $ticketModel->_id = $ticketTable->ticket_id;
            $ticketModel->updateExchangesValues($data, $ticketTable);
        }

        // #29157: Add tags from account
        $tags = $account ? $account->getTicketTags() : [];
        $ticketModel->saveTags($ticketTable->ticket_id, $tags);

        // Log as automatically set as private
        if ($ticketTable->ticket_private) {
            TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Ticket group marked as Private automatically, because it has zero price', null, null, $ticketTable->ticket_id);
        }

        // Find detached e-ticket by parsed data, broker_id and event_id and attach them if possible
        /** @var LocalticketsModelEticketcollection $collection */
        $collection = TicketModel::getCollectionInstance('eticket');
        $collection->addFilter(['ticket_id', "IS NULL"]);
        $collection->addFilter(['broker_id', '= ' . $ticketTable->ticket_broker_id]);
        $collection->addFilter(['parsed_section', "= " . $collection->Quote($ticketTable->ticket_section)]);
        $collection->addFilter(['parsed_row', "= " . $collection->Quote($ticketTable->ticket_row)]);
        $collection->addFilter(['parsed_seat', "IN (" . implode(',', $seatsArray) . ")"]);
        $collection->addFilter(['event_id', "= " . $ticketTable->ticket_event_id]);
        $collection->setLoadCounts(false);
        $collection->load();
        /** @var LocalticketsModelEticket $eticket */
        $etickets = $collection->getItems();
        if ($etickets) foreach ($etickets as $eticket) {
            $seat = $eticket->get('parsed_seat');
            if (!$eticket->attachToTicket($ticketTable->getId(), $seat)) {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::WARNING, 'Could not re-attach previously detached e-ticket (ID ' . $eticket->getId() . ') for seat ' . $seat . ': ' . $eticket->getError(), null, $ticketTable->ticket_broker_id, $ticketTable->getId());
            } else {
                TicketsLogger::log(TicketsLogger::TMSYNC_IMPORT, TicketsLogger::INFO, 'Re-attached previously detached e-ticket (ID ' . $eticket->getId() . ') for seat ' . $seat, null, $ticketTable->ticket_broker_id, $ticketTable->ticket_id);
            }
        }

        return $ticketTable;
    }

    /**
     * Sets Auto PO settings to given ticket group from given TM account
     *
     * @param TableTicket $ticketTable
     * @param LocalticketsModelTmsync_account $account
     */
    private function setTicketAutoPoOptions($ticketTable, $account)
    {
        $ticketTable->ticket_split = $account->get('po_split') ? TicketSplit::SPLIT_TYPE_NO_SINGLES : TicketSplit::SPLIT_TYPE_NO_SPLIT;
        $ticketTable->ticket_shipping_method = (int)$account->get('po_shipping_method');

        // Always create tickets with zero price (#26772) and Public (#26815)
        $ticketTable->ticket_private = 0;

        // #29191: Set price only for new tickets, don't reset it for existing ones
        if (!$ticketTable->ticket_id) {
            $ticketTable->ticket_price = 0;
            $ticketTable->ticket_price_selling = $ticketTable->ticket_price;
        }
    }

    /**
     * Calculates total cost for each item including fees, tax and delivery cost.
     * Uses temporary order model to avoid code duplication and keep the calculation consistent.
     *
     * @param LocalticketsModelTmsync_item[] $items
     */
    private function precalculateItemsTotalCost($items)
    {
        /** @var LocalticketsModelOrder2 $order */
        $order = TicketModel::getModelInstance('order2');
        $tax = 0;
        $fees = 0;
        $first = true;
        foreach ($items as $item) {
            /** @var LocalticketsModelOrder2_item $orderItem */
            $orderItem = TicketModel::getModelInstance('order2_item');
            $orderItem->set('price_per_ticket', $item->get('cost'));
            $orderItem->set('quantity', $item->get('quantity'));
            $orderItem->set('_sync_item', $item);
            $order->addItem($orderItem);

            $tax += $item->get('tax') * $item->get('quantity');
            $fees += $item->get('fee') * $item->get('quantity');

            // Add total order fees and delivery from first item
            if ($first) {
                $fees += $item->get('total_fees');

                /** @var LocalticketsModelOrder2_shipping $shipping */
                $shipping = TicketModel::getModelInstance('order2_shipping');
                $shipping->set('shipping_cost', round($item->get('total_delivery'), 2));
                $order->setPrimaryShipping($shipping);

                $first = false;
            }
        }
        $order->set('fee_api', round($fees, 2));
        $order->set('tax', round($tax, 2));

        // Calculate totals
        $order->collectTotals();

        // Calculate total cost for each item
        foreach ($order->getItems() as $orderItem) {
            $additionalCost = $order->getAdditionalCostForOneTicket($orderItem);
            $item = $orderItem->get('_sync_item');
            $item->set('total_cost', round($item->get('cost') + $additionalCost, 2));
        }

        $order->releaseReferences();
    }
}
